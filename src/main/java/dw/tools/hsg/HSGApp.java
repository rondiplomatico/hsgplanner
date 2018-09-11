package dw.tools.hsg;

import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import dw.tools.hsg.Dienst.Typ;
import scala.Tuple2;

/**
 * <pre>
 * Anforderungen:
 *    - Auswertung der Arbeitszeit pro Team (aufgeschlüsselt nach Einsatz vor Saisonbeginn und Personenanzahl)
 *    - Spieler und Hallenaufsichten können für beliebige Zeitintervalle nicht verfügbar sein
 *    - Hallendienste am Besten vor eigenen Heimspielen
 *    - Die Dienstzeit an Tagen ohne eigenes Heimspiel ist minimal/es gibt keine
 *    - Festlegbare Initialstunden für Teams (gleichmäßige Verteilung auf Mitglieder)
 *    - Keine Dienste bei Spielen von Jugendtrainern
 *    - Aufsicht und Dienste bevorzugt vor eigenen Heimspielen. Je bevorzugter desto besser der Anschluss passt (x-17:00, 17:00 Treffpunkt).
 *      Jede selber aktive Aufsichtsperson sollte "gleich viel Vorteil" haben.
 *    - Nur festgelegte Teams haben Hallendienste (m/wB, m/wA, F1-3, M1-4)
 *    - Max. Hallendienstzeiten für bestimmte Teams einstellbar
 *
 * Done:
 * - Erstellung von Verkaufs- und Kassendienst-Einteilungen für alle Spieler aus allen Teams
 *   Programm berechnet Dienste automatisch aus Spielplan.
 *
 * - Einstellbare Default-Arbeitszeiten (Verkauf, Kasse, Aufsicht)
 *   Halbstündige Intervalle können über {@link Dienst.Typ#getTimesHS()} pro Typ festgelegt werden.
 *   Das Intervall mit der besten Auflösung der Gesamtzeit des Dienstes an dem Tag wird gewählt.
 *
 * - Keine Dienste 1 Stunde vorm/nach eigenen Heimspielen, 2 Stunden vor/nach eigenen Auswärtsspielen
 *   Über Konstanten {@link #SPERRSTD_VORLAUF_HEIMSPIEL}, {@link #SPERRSTD_NACHLAUF_HEIMSPIEL}, {@link #SPERRSTD_VORLAUF_AUSWÄRTSSPIEL} und {@link #SPERRSTD_NACHLAUF_AUSWÄRTSSPIEL} geregelt
 *   Heimspiel-Feststellung über Hallennummer in {@link #GA}
 *
 * - Kassendienste nur für Spiele von festlegbaren Teams
 *   Statische Liste mit Teamnamen in {@link HSGApp#mitKasse}
 *
 * - Einstellbare Vor/Nachlaufzeiten für Kassendienste, Default 1h vorher bis 30Min nach Spielbeginn
 *   Wird per {@link Dienst.Typ#getVorlaufHS()} und {@link Dienst.Typ#getNachlaufHS()} hartcodiert
 *
 * - Anzahl der Helfer pro Dienst ist festlegbar, abhängig vom Typ (K/V) und Tageszeit
 *   Wird per {@link Dienst.Typ#getPersonen()} hartcodiert
 *
 * - Gesamtarbeitszeit pro Teammitglied ist ungefähr gleich über die Saison
 * 	 Ziel 1: Gleiche Arbeitszeit für Spieler und Aufsicht über Gesamtsaison
 *
 * - Berücksichtigung von schon geleisteten Arbeitszeiten vorher (Krautfest, EF, ..)
 * 	 Wird über Ziel1 erreicht: Die Gesamtarbeitszeit enthält neben der potentiellen Neuen auch die importierte aus der Personenliste
 *
 * - Festlegbare Personen für Hallenaufsichten
 *   Personenimportliste kennt "HA" als "Aufsichtsteam", und vierte Spalte mit "x" als Aufsichtsmarkierung.
 *   Aufsichtspersonen werden exklusiv Aufsichtsdiensten zugeordnet. Durch den Teambezug werden unmögliche
 *   Aufsichtsdienste durch Auswärtsspiele ebenfalls vermieden.
 *
 * - Arbeitszeit der Hallenaufsichten ist auch ca. gleich
 *   Analoges Vorgehen über Ziel1, eingeschränkt auf Aufsichtsdienste
 *
 * - Hallenaufsichten können Teammitglieder sein
 *   Gesonderte Markierung im Personenimport erlaubt doppelte Zugehörigkeit und Behandlung aller Konstellationen.
 *
 * - Aktive und Jugend werden berücksichtigt
 *   Alle Teams im Personenimport werden berücksichtigt. Konvertierung der Teamnamen (mit Hilfe der Staffel) in M1, F1, F2, etc
 *
 * </pre>
 */
public class HSGApp {

	private static final long SPERRSTD_NACHLAUF_AUSWÄRTSSPIEL = 3L;
	private static final long SPERRSTD_NACHLAUF_HEIMSPIEL = 2L;
	private static final long SPERRSTD_VORLAUF_AUSWÄRTSSPIEL = 2L;
	private static final long SPERRSTD_VORLAUF_HEIMSPIEL = 1L;
	public static final String GA = "4066";
	public static final List<String> mitKasse = new ArrayList<>(Arrays.asList("F1", "F2", "M1"));

	public static void main(final String[] args) throws IOException, ParseException {
		ConsoleAppender console = new ConsoleAppender();
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.ERROR);
		console.activateOptions();
		Logger.getRootLogger().addAppender(console);
		FileAppender fa = new FileAppender();
		fa.setFile("convert.log");
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		Logger.getRootLogger().addAppender(fa);

		Options options = new Options();
		//
		// Option option = new Option("s", "schema", false, "print the schema");
		// option.setRequired(false);
		// options.addOption(option);
		//
		// option = new Option("o", "out", true, "sets the output file");
		// option.setRequired(false);
		// options.addOption(option);
		//
		// option = new Option("a", "all", false, "flatten all non-array fields");
		// option.setRequired(false);
		// options.addOption(option);
		//
		// option = new Option("f", "fields", true, "sets the field selection, separated
		// by comma");
		// option.setRequired(false);
		// options.addOption(option);
		//
		// option = new Option("r", "repartition", true, "repartitions the parquet
		// file");
		// option.setRequired(false);
		// options.addOption(option);
		//
		// option = new Option("ro", "repartition_object", true, "repartitions and
		// object file");
		// option.setRequired(false);
		// options.addOption(option);

		// CommandLineParser parser = new PosixParser();
		// CommandLine cmd = parser.parse(options, args);
		// if (args.length == 0 || cmd.getArgs().length == 0) {
		// HelpFormatter formatter = new HelpFormatter();
		// formatter.printHelp("<options> <input path>", options);
		// System.exit(0);
		// }

		// Path in = new Path(cmd.getArgs()[0]);
		// Path out = new Path(in.getParent(), "converted" + (cmd.hasOption("f") ||
		// cmd.hasOption("a") ? ".csv" :
		// ".json"));
		// if (cmd.hasOption("o")) {
		// out = new Path(cmd.getOptionValue("o"));
		// }

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("HSG Tool").set("spark.ui.enabled", "false").set("spark.eventLog.enabled",
				"false").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		Path in = new Path(args[1]);
		JavaRDD<Person> personen = jsc.textFile(in.toString()).map(Person::parse).cache();

		in = new Path(args[0]);
		JavaRDD<Game> games = jsc.textFile(in.toString()).map(Game::parse).filter(
				g -> HSGDate.TODAY.beforeOrEquals(g.getDate())).cache();

		run(games, personen);

		jsc.close();

	}

	public static boolean run(final JavaRDD<Game> games, final JavaRDD<Person> personen) {

		HashMap<String, List<Person>> teams = new HashMap<>(
				personen.keyBy(p -> p.getTeamId()).groupByKey().mapValues(IterableUtil::toList).collectAsMap());
		// System.out.println(teams);

		// System.out.println("Spiele:");
		// games.foreach(System.out::println);

		JavaPairRDD<HSGDate, HSGInterval> spielZeiten = games.filter(g -> g.isHeimspiel()).keyBy(
				g -> g.getDate()).aggregateByKey(HSGInterval.MAXMIN, (ex, g) -> ex.stretch(g.getZeit()),
						(a, b) -> a.merge(b));
		// System.out.println("Spielzeiten:");
		// spielZeiten.foreach(System.out::println);

		JavaPairRDD<HSGDate, Spieltag> verkauf = spielZeiten.mapToPair(t -> {
			Spieltag sp = new Spieltag();
			sp.datum = t._1;
			sp.erstesSpiel = t._2.getStart();
			sp.letztesSpiel = t._2.getEnd();
			HSGInterval total = berechneGesamtzeit(t._2, Typ.Verkauf);
			sp.dienste.addAll(verteileDienste(total, Typ.Verkauf));
			return new Tuple2<>(t._1, sp);
		});
		// System.out.println("Verkaufszeiten:");
		// verkauf.foreach(System.out::println);

		JavaPairRDD<HSGDate, List<Dienst>> aufsichtsZeiten = spielZeiten.mapValues(
				v -> berechneGesamtzeit(v, Typ.Aufsicht)).mapValues(v -> verteileDienste(v, Typ.Aufsicht));
		// System.out.println("Aufsichtszeiten:");
		// aufsichtsZeiten.foreach(System.out::println);

		JavaPairRDD<HSGDate, List<Dienst>> kassenZeiten = games.filter(
				g -> g.isHeimspiel() && mitKasse.contains(g.getTeamId())).keyBy(g -> g.getDate()).aggregateByKey(
						HSGInterval.MAXMIN, (ex, g) -> ex.stretch(g.getZeit()), (a, b) -> a.merge(b)).mapValues(
								v -> berechneGesamtzeit(v, Typ.Kasse)).mapValues(v -> verteileDienste(v, Typ.Kasse));
		// System.out.println("Spieltage mit Kasse:");
		// kassenZeiten.foreach(System.out::println);

		JavaPairRDD<HSGDate, Spieltag> dienste = verkauf.join(kassenZeiten).join(aufsichtsZeiten).mapValues(v -> {
			v._1._1.getDienste().addAll(v._1._2);
			v._1._1.getDienste().addAll(v._2);
			return v._1._1;
		});
		// System.out.println("Spieltage:");
		// dienste.foreach(System.out::println);

		/*
		 * Spielzeiten +- Puffer berechnen, um zu wissen welches Team wann keinen
		 * Arbeitsdienst haben sollte
		 */
		JavaPairRDD<HSGDate, Iterable<Tuple2<String, HSGInterval>>> spieltSelber = games.filter(
				g -> teams.containsKey(g.getTeamId())).mapToPair(g -> {
					HSGInterval zeit = new HSGInterval(
							g.getZeit().minus(
									g.isHeimspiel() ? SPERRSTD_VORLAUF_HEIMSPIEL : SPERRSTD_VORLAUF_AUSWÄRTSSPIEL,
									ChronoUnit.HOURS),
							g.getZeit().plus(
									g.isHeimspiel() ? SPERRSTD_NACHLAUF_HEIMSPIEL : SPERRSTD_NACHLAUF_AUSWÄRTSSPIEL,
									ChronoUnit.HOURS));
					return new Tuple2<>(g.getDate(), new Tuple2<>(g.getTeamId(), zeit));
				}).groupByKey();

		// System.out.println("Eigene Spielzeit:");
		// spieltSelber.foreach(System.out::println);

		JavaPairRDD<HSGDate, Tuple2<Spieltag, List<Tuple2<String, HSGInterval>>>> spieltage = dienste.leftOuterJoin(
				spieltSelber).mapValues(v -> {
					List<Tuple2<String, HSGInterval>> owntimes = new ArrayList<>();
					if (v._2.isPresent()) {
						owntimes.addAll(IterableUtil.toList(v._2.get()));
					}
					Tuple2<Spieltag, List<Tuple2<String, HSGInterval>>> res = new Tuple2<>(v._1, owntimes);
					v._1.dienste.forEach(d -> d.setDatum(v._1.datum));
					return res;
				});
		System.out.println("Spieltage mit eigenen (parallelen) Spielen:");
		spieltage.foreach(s -> System.out.println(s));

		/*
		 * TODO - Zuordnungen für Jugendtrainer bei Auswärtsspielen vermeiden
		 */
		JavaRDD<Zuordnung> zuordnungen = spieltage.flatMap(t -> {
			List<Zuordnung> res = new ArrayList<>();
			for (Dienst d : t._2._1.dienste) {
				List<String> forbidden = t._2._2.stream().filter(v -> v._2.intersects(d.zeit)).map(v -> v._1).collect(
						Collectors.toList());
				for (Entry<String, List<Person>> e : teams.entrySet()) {
					if (!forbidden.contains(e.getKey())) {
						e.getValue().forEach(p -> {
							// Aufsichtspersonen nur Aufsichtsdiensten zuordnen, und nicht-aufsichter nur
							// nicht-aufsichtsdiensten
							if (Typ.Aufsicht == d.getTyp() && p.isAufsicht()
									|| Typ.Aufsicht != d.getTyp() && !p.isAufsicht()) {
								// So viele Zuordnungen wie Personen im Dienst hinzufügen
								for (int i = 0; i < d.getTyp().getPersonen(); i++) {
									res.add(new Zuordnung(p, d, i));
								}
							}
						});
					}
				}
			}
			return res.iterator();
		}).cache();
//		System.out.println("Zuordnungen:");
//		zuordnungen.foreach(z -> System.out.println(z.getPerson() + "@" + z.getDienst() + "/" + z.getNr()));

		spieltage.flatMap(s -> s._2._1.getDienste().iterator()).distinct().keyBy(d -> d).leftOuterJoin(
				zuordnungen.map(z -> z.getDienst()).distinct().keyBy(d -> d)).foreach(t -> {
					if (!t._2._2.isPresent()) {
						System.out.println(t._1 + ": Keine Personen zugeordnet");
					}
				});
		System.out.println("Zuordnungen gesamt: " + zuordnungen.count());
		// zuordnungen.mapToPair(z -> z.getDienst())

		List<Zuordnung> selected = HSGSolver.solve(zuordnungen);
		selected.forEach(System.out::println);

		selected.stream().map(z -> new Tuple2<>(z.getPerson(), z.getDienst().getZeit().dauerInMin())).collect(
				Collectors.groupingBy(t -> t._1)).entrySet().stream().map(e -> new Tuple2<>(e.getKey(),
						e.getKey().getGearbeitetM() + e.getValue().stream().mapToInt(Tuple2::_2).sum())).forEach(
								t -> System.out.println(t));
		return selected.size() > 0;
	}

	private static HSGInterval berechneGesamtzeit(final HSGInterval minMax, final Typ typ) {
		return new HSGInterval(minMax.getStart().minus(typ.getVorlaufHS() * 30, ChronoUnit.MINUTES),
				minMax.getEnd().plus(typ.getNachlaufHS() * 30, ChronoUnit.MINUTES));
	}

	private static List<Dienst> verteileDienste(final HSGInterval range, final Dienst.Typ typ) {
		int durationInHalfHrs = optimaleDienstlänge(range.getStart(), range.getEnd(), typ);
		List<Dienst> dienste = new ArrayList<>();
		Dienst d = typ.newDienst();
		d.typ = typ;
		d.zeit = new HSGInterval(range.getStart(), range.getStart().plus(30 * durationInHalfHrs, ChronoUnit.MINUTES));
		dienste.add(d);
		while (d.zeit.getEnd().isBefore(range.getEnd())) {
			d = typ.newDienst();
			LocalTime von = dienste.get(dienste.size() - 1).getZeit().getEnd();
			LocalTime bis = von.plus(30 * durationInHalfHrs, ChronoUnit.MINUTES);
			bis = bis.isAfter(range.getEnd()) || bis.isBefore(von) ? range.getEnd() : bis;
			d.zeit = new HSGInterval(von, bis);
			dienste.add(d);
		}
		/*
		 * Falls der letzte Dienst weniger als eine Stunde ist, den vorletzten um die
		 * Zeit verlängern und den letzten rausnehmen.
		 */
		Dienst last = dienste.get(dienste.size() - 1);
		if (last.getZeit().dauerInMin() < 60) {
			dienste.get(dienste.size() - 2).setZeit(dienste.get(dienste.size() - 2).getZeit().merge(last.getZeit()));
			dienste.remove(last);
		}
		return dienste;
	}

	private static int optimaleDienstlänge(final LocalTime start, final LocalTime end, final Typ typ) {
		/*
		 * Bestimme die optimale Dienstlänge für den ganzen Tag
		 */
		int startHH = start.getHour() * 2 + start.getMinute() / 30;
		int endHH = end.getHour() * 2 + end.getMinute() / 30;
		int totalDurationInHalfHrs = endHH - startHH;
		double minLoss = Double.MAX_VALUE;
		int durationInHalfHrs = 0;
		for (int halfHrs : typ.getTimesHS()) {
			double nTimes = totalDurationInHalfHrs / (double) halfHrs;
			double loss = Math.abs(nTimes - Math.round(nTimes));
			if (loss < minLoss) {
				minLoss = loss;
				durationInHalfHrs = halfHrs;
			}
		}
		return Math.min(totalDurationInHalfHrs, durationInHalfHrs);
	}

	private static boolean saveAsFile(final JavaRDD<String> data, final Path file) {
		Path tmp = new Path(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
		data.saveAsTextFile(tmp.toUri().toString());
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = tmp.getFileSystem(conf);
			if (fs.exists(file)) {
				fs.delete(file, true);
			}
			FileUtil.copyMerge(fs, tmp, fs, file, true, conf, null);
			fs.delete(tmp, true);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
