package dw.tools.hsg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.google.common.base.Charsets;

import dw.tools.hsg.Dienst.Typ;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Aufrufparameter: <spielliste_hvw> <spielerexport>
 * 
 * spielliste_hvw von
 * http://spo.handball4all.de/Spielbetrieb/mannschaftsspielplaene.php?orgGrpID=3
 * mit Vereinsnummer 114
 * 
 * <pre>
 * Anforderungen:
 *    - Auswertung der Arbeitszeit pro Team (aufgeschlüsselt nach Einsatz vor Saisonbeginn und Personenanzahl)
 *    - Spieler und Hallenaufsichten können für beliebige Zeitintervalle nicht verfügbar sein
 *    - Die Dienstzeit an Tagen ohne eigenes Heimspiel ist minimal/es gibt keine
 *    - Festlegbare Initialstunden für Teams (gleichmäßige Verteilung auf Mitglieder)
 *    - Aufsicht und Dienste bevorzugt vor eigenen Heimspielen.
 *      Jede selber aktive Aufsichtsperson sollte "gleich viel Vorteil" haben.
 *    - Trainer auch einteilen?
 *    - Weniger Dienste für Trainer von Jugendteams?
 *    - Gemischte Verkaufsschichten? (Jugend/Aktive, M/W)
 * Neu seit 20/21
 *    - Keine Dienste für Aktive, welche auch eine Jugend trainieren.
 *    - Arbeitsdienste auch für C+D-Jugend planen (dort eltern rein)
 * Neu seit 21/22   
 *    - e,f Jugend Eltern verplanen
 *     - Für Spieltage drei Schichten bei e,f Jugend
 *    - Stunde vorher, halbe nachher für c, d Jugend Elternschichten
 *     - Die Zeiten einfach als Slot vergeben (keine Optimierung)     
 *    
 * Alternative:
 * - Planung Dienste nur über Team
 * - Berücksichtigung der "Teamleistung" für die Planung (summe über einzelne stunden)
 * - Berücksichtigung der Teamgröße für die Planung
 * - Manuelle Eintragung durch Teamverantwortliche. (Unterstützungsmöglichkeit durch Stundenübersicht pro Team)
 *    
 * Feedback:
 *    - Keine Dienste an spielfreien Wochenenden, [zumindest für aktive Aufsicht]
 *    - Doppelspielrecht berücksichtigen
 *    - Einstellbare Sperrzeiten für Teams (erste 2 Spieltage keine Jugend)
 *    - Maximale Arbeitszeit pro Tag 6h o.ä.
 *
 * Done:
 * - Erstellung von Verkaufs- und Kassendienst-Einteilungen für alle Spieler aus allen Teams
 *   Programm berechnet Dienste automatisch aus Spielplan.
 *
 * - Hallendienste am Besten vor eigenen Heimspielen. Je bevorzugter desto besser der Anschluss passt (x-17:00, 17:00 Treffpunkt). Für Teams sowie Aufsicht
 *   Erledigt über "Begünstigungssystem". Jede Zuordnung hat eine standardmäßige "Strafzeit" von {@link HSGSolver#DEFAULT_ZUORDNUNG_WEIGHT} Minuten, wenn sie gewählt wird.
 *   Ist eine Schicht näher als {@link HSGSolver#CLEVERE_DIENSTE_MAX_ABSTAND_MINUTEN} an einem Heimspiel (inkl. entsprechender Vor/Nachläufe),
 *   entspricht die Strafe dem Abstand in Minuten.
 *   TODO: Aufsichtsschichten übeschneiden die Spielsperrzeiten zu oft, es gibt kaum "optimale" Anschlüsse für Aufsicht.
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
 * - Einstellbare Vor/Nachlaufzeiten für Kassendienste, Default 1h vorher bis 30Min nach Spielbeginn.
 *   Sonderregelung für F1: Die Kassenzeit beginnt schon eine halbe Stunde früher, weil viele Gäste schon eher in die Halle kommen.
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
 * - Nur festgelegte Teams haben Hallendienste.
 *   Alle Teams im Personenimport werden importiert, aber {@link Person#mayWork()} prüft ob diese auch für
 *   die Arbeitsdienste in Frage kommt. Maßgeblich ist die Liste {@link Person#WORKING_TEAMS}.
 *   Konvertierung der Teamnamen (mit Hilfe der Staffel) in M1, F1, F2, etc
 *
 * - Max. Hallendienstzeiten für bestimmte Teams einstellbar
 *   Das Feld {@link Person#WORKING_TEAMS} enthält eine Map mit Arbeitszeit-Intervallen, innerhalb derer alle
 *   Diensteinteilungen für das Team liegen müssen.
 *
 * - Keine Dienste bei Spielen von eigenen Trainern
 *   In Personenexport gibt es eine Spalte, die die Teamnamen enthalten kann, die eine Person trainiert.
 *   Die betroffenen Personen haben dann keine Dienste innerhalb der Sperrzeiten des Spiels (vor/nachlauf).
 *   
 * - Festlegbare "Arbeitskraft"-Anteile für alle Teams. Insbesondere schafft A-Jugend 80% und B-Jugend 60% im Vergleich zu Aktiven.
 *   Methode {@link Team#leistungsFaktor()} gibt pro Team einen Faktor aus [0, 1] zurück, der den prozentualen Anteil am Mittelwert der 
 *   Einzelpersonenarbeitszeit angibt, welcher die Zielgesamtarbeitszeit vorgibt.
 * 
 * </pre>
 */
public class HSGApp {

	/**
	 * Anzahl der Spalten, in denen im HelferlisteHallendienste Helfer eingetragen
	 * werden können. Dies ist mit 6 mehr als die max 2 Personen in Diensten,
	 * ermöglicht aber Copy&Paste des Ergebnis-CSVs in Google Sheets mit korrekter
	 * Spaltenreferenzierung für die Auswertesheets Ü19 etc
	 */
	private static final int ANZ_HELFER_SPALTEN = 6;
	public static final String GA = "4066";
	public static final String HSGLE = "HSG Leinfelden-Echterdingen";
	public static final String CSV_DELIM = ";";
	public static final HSGDate START_DATE = null; // new HSGDate(2019, 11, 1);
	public static final HSGDate END_DATE = null; // new HSGDate(2119, 12, 31); // HSGDate.TODAY.nextDay(1);
	public static final DecimalFormat DEC_FORMAT = new DecimalFormat("##.##");
	public static final List<Team> ELTERN_DIENSTE = Arrays
															.asList(Team.mC1, Team.mC2, Team.wC1, Team.wC2, Team.gD1, Team.gD2, Team.wD1, Team.wD2, Team.gE1, Team.gE2, Team.wE1, Team.wE2, Team.gF1, Team.gF2);
	public static final List<Team> WISCHER_DIENSTE = Arrays.asList(Team.mC1, Team.mC2, Team.wC1, Team.wC2);

	/**
	 * Wenn das erste Spiel F1 ist, startet die Gesamtarbeitszeit
	 * F1_EARLY_START_MINUTES früher.
	 * Auch die Kasse wird bei F1 um F1_EARLY_START_MINUTES früher eingesetzt.
	 */
	private static final int F1_EARLY_START_MINUTES = 30;

	/**
	 * Flag that configures the app to assign teams only instead of separate
	 * players.
	 */
	public static final boolean TEAMS_ONLY = true;

	public static Logger logger = Logger.getLogger(HSGApp.class);

	public static void main(final String[] args) throws IOException, ParseException {
		Path spieleCSVRaw = new Path(args[0]);
		Path spielerCSVRaw = new Path(args[1]);

		/*
		 * Convert to UTF8
		 */
		Path spieleCSV = prepare(spieleCSVRaw, true);
		Path spielerCSV = prepare(spielerCSVRaw, false);

		Path outbase = args.length > 2 ? new Path(args[2]) : spielerCSV.getParent();

		FileAppender fa = new FileAppender();
		fa.setFile(new Path(outbase, "convert.log").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		Logger.getRootLogger().addAppender(fa);

		SparkConf sparkConf = new SparkConf();
		sparkConf	.setAppName("HSG Tool")
					.set("spark.ui.enabled", "false")
					.set("spark.eventLog.enabled", "false")
					.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// Personen einlesen
		JavaRDD<Person> personen = jsc	.textFile(spielerCSV.toString())
										.map(Person::parse)
										.filter(p -> p != null)
										.filter(Person::mayWork)
										.cache(); // Remove any person that may not work
		personen.foreach(p -> logger.debug("Person " + p));
		/*
		 * Hack für Jugendteams - hier wird angenommen, es gibt immer 10 Leute.
		 */
		if (TEAMS_ONLY) {
			List<Person> fake = new ArrayList<>();
			personen.filter(p -> p.getTeam().isJugend() && Team.Aufsicht != p.getTeam())
					.keyBy(Person::getTeam)
					.countByKey()
					.forEach((t, cnt) -> {
						for (long i = cnt; i < 10; i++) {
							fake.add(new Person(t.name() + "#" + i, "#" + i, t, 0, false, null));
						}
					});
			personen = personen.union(jsc.parallelize(fake));
		}

		// Spiele einlesen
		JavaRDD<Game> games = jsc	.textFile(spieleCSV.toString())
									.map(Game::parse)
									.filter(g -> g != null
											&& (START_DATE == null || g.getDate().after(START_DATE))
											&& (END_DATE == null || g.getDate().before(END_DATE)))
									// .sortBy(g -> g, true, 1)
									// .repartition(6)
									.cache();
		games	.filter(g -> g.isHeimspiel())
				.foreach(g -> logger.info("Spiel:" + g));
		logger.info("Importierte Spiele: " + games.count() + ", davon Heimspiele: "
				+ games	.filter(g -> g.isHeimspiel())
						.count());

		JavaPairRDD<HSGDate, Spieltag> spieltage = berechneSpieltage(games, personen);

		final Map<Typ, Integer> zeitenNachTyp = new HashMap<>(
				spieltage	.flatMap(s -> s._2.getDienste().iterator())
							.filter(d -> !d.isElternVerkaufsDienst())
							.distinct()
							.keyBy(d -> d.getTyp())
							.aggregateByKey(0, (
									ex, n) -> ex + n.zeit.dauerInMin() * n.getTyp().getPersonen(), (a, b) -> a + b)
							.collectAsMap());
		zeitenNachTyp.forEach((t,
				z) -> logger.info("Gesamt zu leistende Zeit für " + t + ": " + z + " (" + DEC_FORMAT.format(z / 60.0)
						+ "h)"));

		final Map<Team, Double> avgZeitProTeam = berechneSpielerArbeitszeit(personen, zeitenNachTyp);

		int kürzesterDienst =
				Arrays.asList(Dienst.Typ.values()).stream().mapToInt(t -> t.getTimesHS()[0]).min().getAsInt()
						* 30;

		if (TEAMS_ONLY) {
			/*
			 * Bei nur Teambasierter Berechnung nehmen wir einfach einen representativen
			 * Spieler pro Team, der die Summe aller Teamvorarbeiten mitbekommt.
			 */
			final Map<Team, Integer> vorleistungProTeam = new HashMap<>(
					personen.keyBy(p -> p.getTeam())
							.aggregateByKey(0, (ex, n) -> ex + n.getGearbeitetM(), (a, b) -> a + b)
							.collectAsMap());
			personen = personen.map(Person::teamRepresentant).distinct().map(p -> {
				p.setGearbeitetM(vorleistungProTeam.get(p.getTeam()));
				return p;
			}).cache();
		} else {
			// Alle Spieler rauswerfen, die schon genug gearbeitet haben (reduziert die
			// Problemkomplexität)
			personen = personen.filter(p -> {
				boolean valid = p.getGearbeitetM() < avgZeitProTeam.get(p.getTeam()) - kürzesterDienst / 2;
				if (!valid) {
					logger.info(p + " hat mit " + DEC_FORMAT.format(p.getGearbeitetM() / 60.0)
							+ "h mehr (oder ist hinreichend nah dran) als der erforderliche Durchschnitt von "
							+ DEC_FORMAT.format(avgZeitProTeam.get(p.getTeam()) / 60.0)
							+ "h gearbeitet und wird nicht mehr eingeteilt.");
					return false;
				}
				valid = !(p.getTeam().isAktive() && p.getTrainerVon() != null && p.getTrainerVon().isJugend());
				if (!valid) {
					logger.info(p + " ist von den Arbeitsdiensten ausgeschlossen, weil aktiv bei " + p.getTeam()
							+ " und Jugendtrainer von " + p.getTrainerVon());
					return false;
				}
				return true;
			}).cache();
		}

		JavaRDD<Zuordnung> alleMöglichenZuordunungen = erzeugeZuordnungen(personen, spieltage).cache();

		JavaRDD<Zuordnung> elternDienste = alleMöglichenZuordunungen.filter(z -> z.isFixed()).cache();
		logger.info("Anzahl Zuordnungen: " + alleMöglichenZuordunungen.count()
				+ ", darin feste Zuordnungen durch Elterndienste: " + elternDienste.count());

		JavaRDD<Zuordnung> gültigeZuordnungen =
				entferneDiensteAnNichtspieltagen(games, alleMöglichenZuordunungen.filter(z -> !z.isFixed()), spieltage);

//		JavaRDD<Zuordnung> gültigeZuordnungen = alleMöglichenZuordunungen;

		if (!checkAlleNotwendigenZuordnungenSindVorhanden(spieltage, gültigeZuordnungen).isEmpty()) {
			jsc.close();
			throw new RuntimeException("Zu wenige Zuordnungen generiert!");
		}

//		JavaRDD<Zuordnung> teamZuordnungen =
//				gültigeZuordnungen	.filter(z -> !z.getDienst().getTyp().equals(Typ.Aufsicht))
//									.map(z -> {
//										Person p = new Person(z.getPerson().getTeam().name(), z.getPerson().getTeam(),
//												0, z.getPerson()
//													.isAufsicht(),
//												null);
//										return new Zuordnung(p, z.getDienst(), z.getNr(), 0);
//									})
//									.distinct()
//									.cache();
//		teamZuordnungen.sortBy(z -> z.toString(), true, 1).foreach(z -> System.out.println(z));
//		gültigeZuordnungen = teamZuordnungen;

		/*
		 * Fixierte Zuordnungen einlesen und rausrechnen
		 */
//		if (args.length > 3) {
//			gültigeZuordnungen = processFixed(args, jsc, personen, gültigeZuordnungen);
//		}

		JavaRDD<String> content = toCSV(games, gültigeZuordnungen.union(elternDienste));
		Path out = new Path(outbase, "dienste_rahmen.csv");
		saveAsFile(content, out);

//		final Map<Typ, Integer> zeitenNachTyp = new HashMap<>(
//				spieltage.flatMap(s -> s._2.dienste.iterator()).distinct().keyBy(d -> d.getTyp()).aggregateByKey(0,
//						(ex, n) -> ex + n.zeit.dauerInMin() * n.getTyp().getPersonen(),
//						(a, b) -> a + b).collectAsMap());
//		zeitenNachTyp.forEach(
//				(t, z) -> logger.info("Gesamt zu leistende Zeit für " + t + ": " + z + " (" + (z / 60.0) + "h)"));

		JavaRDD<Zuordnung> zu = jsc	.parallelize(HSGSolver.solve(gültigeZuordnungen, games, avgZeitProTeam))
									.union(elternDienste)
									.cache();

//		if (TEAMS_ONLY) {
//			zu.flatMap(z -> {
//				
//				z.getDienst().getTyp().getPersonen();
//			});
//		}

		zu	.filter(z -> z.getPerson().isAufsicht())
			.keyBy(z -> z.getPerson())
			.aggregateByKey(0, (ex, n) -> ex + n.getDienst().getZeit().dauerInMin(), (a, b) -> a + b)
			.foreach(v -> logger.warn(v._1 + ": " + v._2));

		content = toCSV(games, zu);
		out = new Path(outbase, "dienste.csv");
		saveAsFile(content, out);

		exportStats(outbase, zu, personen);

		jsc.close();

	}

	private static Path prepare(Path in, boolean convert) {
		Path res = new Path(in.getParent(), "utf8-" + in.getName());
		try {
			String str = new String(Files.readAllBytes(Paths.get(in.toString())), convert ? Charsets.ISO_8859_1
					: Charsets.UTF_8);
			str = str.replace(",", ";");
			Files.write(Paths.get(res.toString()), str
														.getBytes(Charsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return res;
	}

	private static Path convertToISO(Path in) {
		Path res = new Path(in.getParent(), "iso-" + in.getName());
		try {
			String str = new String(Files.readAllBytes(Paths.get(in.toString())), Charsets.UTF_8);
			Files.write(Paths.get(res.toString()), str
														.getBytes(Charsets.ISO_8859_1), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return res;
	}

	private static Map<Team, Double> berechneSpielerArbeitszeit(JavaRDD<Person> personen,
			Map<Typ, Integer> zeitenNachTyp) {

		final Map<Team, Tuple2<Integer, Double>> anzahlUndVorleistungProTeam = new HashMap<>(
				personen.keyBy(p -> p.getTeam())
						.aggregateByKey(new Tuple2<Integer, Double>(
								0, 0.0), (ex, n) -> new Tuple2<>(ex._1 + 1, ex._2 + n.getGearbeitetM()), (a,
										b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
						.collectAsMap());
		anzahlUndVorleistungProTeam.forEach((t, v) -> logger.info("Vorleistung von Team " + t + " mit " + v._1
				+ " Spielern: "
				+ v._2 + " (" + (v._2 / 60.0) + "h)"));
		final double gesamtVorleistung = anzahlUndVorleistungProTeam.values().stream().mapToDouble(v -> v._2).sum();
		logger.info("Gesamtvorleistung " + gesamtVorleistung / 60.0 + "h");

		/*
		 * Zähle die Gesamtanzahl effektiver Personen (manche Teams haben einen
		 * Leistungsfaktor)
		 */
		double gesamtEffektivPersonen =
				anzahlUndVorleistungProTeam	.entrySet()
											.stream()
											.filter(t -> !Team.Aufsicht.equals(t.getKey()))
											.mapToDouble(e -> e.getKey().getLeistungsFaktor() * e.getValue()._1)
											.sum();
		final double zeitProPerson = (zeitenNachTyp.get(Typ.Kasse) + zeitenNachTyp.get(Typ.Verkauf) + zeitenNachTyp
																													.get(Typ.Wischen))
				/ gesamtEffektivPersonen;
		logger.info("Arbeitszeit für " + gesamtEffektivPersonen
				+ " volle Personen an Kasse, Verkauf und Wischern im Schnitt "
				+ zeitProPerson
				+ "min (" + DEC_FORMAT.format(zeitProPerson / 60.0) + "h)");
		final double zeitProAufsicht = (zeitenNachTyp.get(Typ.Aufsicht)) / anzahlUndVorleistungProTeam
																										.get(Team.Aufsicht)._1;
		logger.info("Arbeitszeit für " + anzahlUndVorleistungProTeam.get(Team.Aufsicht)._1
				+ " Aufsichtsmitglieder im Schnitt " + zeitProAufsicht
				+ "min (" + DEC_FORMAT.format(zeitProAufsicht / 60.0) + "h)");

		Map<Team, Double> res = new HashMap<>(
				anzahlUndVorleistungProTeam	.entrySet()
											.stream()
											.collect(Collectors.toMap(e -> e.getKey(), e -> {
												double gesamtZeit = e.getKey().getLeistungsFaktor() * e.getValue()._1
														* (Team.Aufsicht.equals(e.getKey()) ? zeitProAufsicht
																: zeitProPerson);
												double schnittProSpieler = gesamtZeit / (double) e.getValue()._1;
												logger.info("Team " + e.getKey() + " muss die Saison "
														+ DEC_FORMAT.format(gesamtZeit / 60.0)
														+ "h arbeiten und hat schon "
														+ DEC_FORMAT.format(e.getValue()._2 / 60.0)
														+ "h geleistet. Bleiben "
														+ DEC_FORMAT.format((gesamtZeit - e.getValue()._2) / 60.0)
														+ "h, macht "
														+ DEC_FORMAT.format(schnittProSpieler / 60.0)
														+ "h im Schnitt für "
														+ e.getValue()._1
														+ " Spieler");
												return schnittProSpieler;
											})));
		return res;
	}

	private static JavaRDD<Zuordnung> processFixed(final String[] args, JavaSparkContext jsc, JavaRDD<Person> personen,
			JavaRDD<Zuordnung> allzu) {
		Path in;
		in = new Path(args[3]);
		JavaRDD<Zuordnung> fixed =
				jsc	.textFile(in.toString())
					.flatMap(l -> Zuordnung.read(l).iterator())
					.keyBy(z -> new Tuple2<>(
							z.getPerson().getName(),
							z.getPerson().getTeam()))
					.leftOuterJoin(personen.keyBy(p -> new Tuple2<>(
							p.getName(), p.getTeam())))
					.map(d -> {
						if (d._2._2.isPresent()) {
							return new Zuordnung(d._2._2.get(), d._2._1.getDienst(), d._2._1.getNr());
						} else {
							logger.warn("nicht gefunden: " + d._1 + " in " + d._2._1);
							return null;
						}
					})
					.filter(d -> d != null)
					.cache();
		fixed.foreach(f -> logger.info("Festgelegt: " + f));

		allzu = allzu	.keyBy(z -> new Tuple3<>(z.getDienst(), z.getPerson(),
				z.getNr()))
						.fullOuterJoin(fixed.keyBy(z -> new Tuple3<>(z.getDienst(), z.getPerson(),
								z.getNr())))
						.map(d -> {
							if (d._2._1.isPresent()) {
								Zuordnung z = d._2._1.get();
								z.setFixed(d._2._2.isPresent());
								if (d._2._2.isPresent()) {
									logger.warn("Zuordnung fixiert: " + z);
								}
								return z;
							} else {
								logger.warn("Keine Zuordnung für Fixierung: " + d._2._2.get().getDienst().getDatum()
										+ ", "
										+ d._2._2.get());
								return null;
							}
						})
						.filter(d -> d != null);
		return allzu;
	}

	/**
	 * @param in
	 * @param zu
	 * @param personen
	 * @param personen
	 */
	private static void exportStats(final Path out, final JavaRDD<Zuordnung> zu, final JavaRDD<Person> personen) {
		JavaPairRDD<Person, Integer> effectiveWorkTime =
				zu	.mapToPair(z -> new Tuple2<>(z.getPerson(), z.getDienst()
																.getZeit()
																.dauerInMin()))
					.reduceByKey((a, b) -> a + b)
					.cache();
		effectiveWorkTime.foreach(t -> logger.info("Effektive Arbeitszeit von " + t._1 + ": " + t._2));
		final Tuple2<Integer, Integer> avgAufsichtT =
				effectiveWorkTime	.filter(z -> z._1.isAufsicht())
									.isEmpty()
											? new Tuple2<>(0, 1)
											: effectiveWorkTime	.filter(z -> z._1.isAufsicht())
																.map(z -> new Tuple2<>(z._2, 1))
																.reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2
																		+ b._2));
		final Tuple2<Integer, Integer> avgDienstT =
				effectiveWorkTime	.filter(z -> !z._1.isAufsicht())
									.isEmpty()
											? new Tuple2<>(0, 1)
											: effectiveWorkTime	.filter(z -> !z._1.isAufsicht())
																.map(z -> new Tuple2<>(z._2, 1))
																.reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2
																		+ b._2));
		Map<Team, Integer> teamGröße =
				new HashMap<>(zu.map(z -> z.getPerson())
								.filter(p -> !p.isAufsicht())
								.distinct()
								.mapToPair(p -> new Tuple2<>(p.getTeam(), 1))
								.reduceByKey((a, b) -> a + b)
								.collectAsMap());
		final double avgAufsicht = avgAufsichtT._1 / (double) avgAufsichtT._2;
		final double avgDienst = avgDienstT._1 / (double) avgDienstT._2;
		logger.warn("Durchschn. Arbeitszeit Normal: " + avgDienst + ", Aufsicht:" + avgAufsicht);

		/*
		 * Zeiten pro Person
		 */
		JavaRDD<String> stats1 = effectiveWorkTime.map(t -> {
			int total = t._1.getGearbeitetM() + t._2;
			return String.join(HSGApp.CSV_DELIM, t._1.getName()
					+ (t._1.isAufsicht() ? " (A)" : ""), t._1.getTeam().toString(), "" + t._1.getGearbeitetM(), ""
							+ t._2, Integer.toString(total), Double	.toString(total
									- (t._1.isAufsicht() ? avgAufsicht : avgDienst))
																	.replace('.', ','));
		}).sortBy(s -> s, true, 1);

		/*
		 * Zeiten pro Team
		 */
		JavaRDD<String> stats2 =
				effectiveWorkTime	.filter(z -> !z._1.isAufsicht())
									.mapToPair(z -> new Tuple2<>(
											z._1.getTeam(), new Tuple2<>(z._1.getGearbeitetM(), z._2)))
									.reduceByKey((a, b) -> new Tuple2<>(
											a._1 + b._1,
											a._2 + b._2))
									.map(t -> String.join(HSGApp.CSV_DELIM, "Gesamtsumme Team", t._1.toString(), Integer.toString(t._2._1), Integer.toString(t._2._2), Integer.toString(t._2._1
											+ t._2._2), Integer.toString(teamGröße.get(t._1)), Double
																										.toString((t._2._1
																												+ t._2._2)
																												/ (double) teamGröße.get(t._1))
																										.replace('.', ',')))
									.sortBy(s -> s, true, 1);
		;

		/*
		 * Personen ohne Arbeitsdienste
		 */
		JavaRDD<String> stats3 =
				personen.keyBy(p -> p)
						.leftOuterJoin(zu.mapToPair(z -> new Tuple2<>(z.getPerson(),
								1)))
						.map(d -> {
							if (!d._2._2.isPresent()) {
								return String.join(HSGApp.CSV_DELIM, "Kein Dienst", d._1.getName()
										+ (d._1.isAufsicht() ? " (A)"
												: ""), d._1	.getTeam()
															.toString(), Integer.toString(d._1.getGearbeitetM()), Double
																														.toString(d._1.getGearbeitetM()
																																- (d._1.isAufsicht()
																																		? avgAufsicht
																																		: avgDienst))
																														.replace('.', ','));
							} else {
								return null;
							}
						})
						.filter(s -> s != null)
						.sortBy(s -> s, true, 1);
		saveAsFile(stats1.union(stats2).union(stats3), new Path(out, "stats.csv"));
	}

	public static JavaRDD<String> toCSV(final JavaRDD<Game> games, final JavaRDD<Zuordnung> zu) {
		return zu	.keyBy(z -> z.getDienst().getDatum())
					.groupByKey()
					.mapValues(t -> {
						List<Tuple2<Dienst, List<Zuordnung>>> hlp = new ArrayList<>(
								StreamSupport	.stream(t.spliterator(), false)
												.collect(Collectors.groupingBy(z -> z.getDienst()))
												.entrySet()
												.stream()
												.map(e -> new Tuple2<>(
														e.getKey(), e.getValue()))
												.collect(Collectors.toList()));
						Collections.sort(hlp, (a, b) -> a._1.compareTo(b._1));
						return hlp;
					})
					.join(games.keyBy(g -> g.getDate()).groupByKey().mapValues(v -> {
						List<Game> res = IterableUtil.toList(v);
						Collections.sort(res);
						return res;
					}))
					.sortByKey()
					.flatMap(t -> {
						// Spielinfo (4 spalten) Datum Von Bis Was Team Anzahl Helfer 1 Helfer 2
						List<String> res = new ArrayList<>();
						int max = Math.max(t._2._1.size(), t._2._2.size());
						for (int i = 0; i < max; i++) {
							List<String> elems = new ArrayList<>(14);
							// @@@@ Dienst
							elems.add(t._1.toddMMyyyy()); // Datum
							if (i < t._2._1.size()) {
								Tuple2<Dienst, List<Zuordnung>> d = t._2._1.get(i);
								elems.add(d._1.toCSV()); // 3 columns (von, bis, was)
								elems.add(d._2.get(0).getPerson().getTeam().name()); // team
								int np = d._2.get(0).getDienst().getTyp().getPersonen();
								elems.add(Integer.toString(np)); // Immer die Richtige anzahl ausgeben.
								if (!TEAMS_ONLY || d._1.getTyp() == Typ.Aufsicht || d._1.isElternVerkaufsDienst()) {
									// Personenliste - immer 5 Personen (einheitliches Format in der Helferliste)
									d._2.subList(0, np)
										.stream()
										.map(z -> d._1.isElternVerkaufsDienst() ? "Eltern " + d._1.getParentsOf()
												: z.getPerson().getName())
										.forEach(elems::add);
								} else {
									np = 0;
								}
								// Leere Felder für die 3-4 restlichen slots
								for (int zz = 0; zz < ANZ_HELFER_SPALTEN - np; zz++) {
									elems.add("");
								}
							} else { // oder eine leere Zeile
								elems.add(HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM
										+ HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM
										+ HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
							}
							// Add 3 columns for time bookkeeping (";;" -> ";;;;"
							elems.add(HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
							// @@@@ Spiel
							// Entweder die Spieldaten, oder eine leere Zeile
							elems.add(i < t._2._2.size() ? t._2._2.get(i).toCSV()
									: HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
							res.add(String.join(HSGApp.CSV_DELIM, elems));
						}
						return res.iterator();
					});
	}

	public static JavaPairRDD<HSGDate, Spieltag> berechneSpieltage(final JavaRDD<Game> games,
			final JavaRDD<Person> personen) {

		JavaPairRDD<HSGDate, Iterable<Tuple2<Person, HSGInterval>>> blockierteTrainer =
				personen.filter(t -> t.getTrainerVon() != null)
						.keyBy(Person::getTrainerVon)
						.join(games.keyBy(Game::getTeam))
						.mapToPair(t -> new Tuple2<>(
								t._2._2.getDate(),
								new Tuple2<>(t._2._1, t._2._2.getDienstSperrenZeitraum())))
						.groupByKey();
		// System.out.println("Blockierte Trainer:");
		// blockierteTrainer.foreach(System.out::println);

		/*
		 * Spielzeiten
		 */
		JavaPairRDD<HSGDate, Spielzeiten> spielZeiten =
				games	.filter(g -> g.isHeimspiel())
						.groupBy(g -> g.getDate())
						.mapValues(t -> berechneDienstzeiten(t));
		// System.out.println("Spielzeiten:");
		// spielZeiten.foreach(s -> System.out.println(s));

		/*
		 * Verkauf
		 */
		JavaPairRDD<HSGDate, Spieltag> verkauf = spielZeiten.mapToPair(t -> {
			Spieltag sp = new Spieltag();
			sp.datum = t._1;

			if (!HSGInterval.EMPTY.equals(t._2.spielerVerkaufsZeit)) {
				HSGInterval total = berechneGesamtzeit(t._2.spielerVerkaufsZeit, Typ.Verkauf);
				sp.dienste.addAll(verteileDienste(total, Typ.Verkauf));
			}
			if (!t._2.elternVerkaufsZeiten.isEmpty()) {
				sp.dienste.addAll(erstelleElternDienste(t._1, t._2.elternVerkaufsZeiten));
			}
			return new Tuple2<>(t._1, sp);
		});
		// System.out.println("Verkaufszeiten:");
		// verkauf.foreach(System.out::println);

		/*
		 * Aufsicht
		 */
		JavaPairRDD<HSGDate, List<Dienst>> aufsichtsZeiten =
				spielZeiten	.mapValues(sz -> sz.getGesamtZeit())
							.mapValues(v -> berechneGesamtzeit(v, Typ.Aufsicht))
							.mapValues(v -> verteileDienste(v, Typ.Aufsicht));
		// System.out.println("Aufsichtszeiten:");
		// aufsichtsZeiten.foreach(System.out::println);

		/*
		 * Kasse Sonderlocke F1: Der Start der Spielzeit wird dann künstlich eine halbe
		 * stunde nach vorne gelegt, damit die Kassenschicht früher anfängt.
		 */
		JavaPairRDD<HSGDate, List<Dienst>> kassenZeiten = games	.filter(g -> g.isHeimspiel()
				&& g.getTeam().isMitKasse())
																.keyBy(g -> g.getDate())
																.aggregateByKey(HSGInterval.MAXMIN, (ex,
																		g) -> g.getTeam().equals(Team.F1)
																				&& g.getZeit()
																					.compareTo(ex.getStart()) <= 0
																							? ex.stretch(g	.getZeit()
																											.minusMinutes(F1_EARLY_START_MINUTES))
																							: ex.stretch(g.getZeit()), (
																									a,
																									b) -> a.merge(b))
																.mapValues(v -> berechneGesamtzeit(v, Typ.Kasse))
																.mapValues(v -> verteileDienste(v, Typ.Kasse));
		// System.out.println("Spieltage mit Kasse:");
		// kassenZeiten.foreach(System.out::println);

		/*
		 * Wischer
		 */
		JavaPairRDD<HSGDate, List<Dienst>> wischerDienst = games.filter(g -> g.isHeimspiel() && g	.getTeam()
																									.isMitWischer())
																.groupBy(g -> g.getDate())
																.mapValues(t -> {
																	List<Game> wl = IterableUtil.toList(t);
																	if (wl.size() == 1) {
																		Game g = wl.get(0);
																		LocalTime bis = g	.getZeit()
																							.plus(60, ChronoUnit.MINUTES);
																		HSGInterval time = new HSGInterval(g.getZeit(),
																				bis);
																		Dienst d = new Dienst(g.getDate(), time,
																				Typ.Wischen, Team.None);
																		return Collections.singletonList(d);
																	}
																	throw new RuntimeException(
																			"Zwei Wischerdienste pro Spieltag aktuell nicht implementiert.");
																});

		/*
		 * Alle Dienste
		 */
		JavaPairRDD<HSGDate, Spieltag> dienste =
				verkauf	.join(aufsichtsZeiten)
						.leftOuterJoin(kassenZeiten)
						.leftOuterJoin(wischerDienst)
						.mapToPair(t -> {
							HSGDate tag = t._1;
							Tuple2<Tuple2<Tuple2<Spieltag, List<Dienst>>, Optional<List<Dienst>>>, Optional<List<Dienst>>> v =
									t._2;
							// aufsicht
							v._1._1._1.getDienste().addAll(v._1._1._2);
							logger.debug("Spieltag " + tag + ": Füge " + v._1._1._2.size() + " Aufsichtsdienste hinzu");
							// kassen
							if (v._1._2.isPresent()) {
								v._1._1._1.getDienste().addAll(v._1._2.get());
								logger.debug("Spieltag " + tag + ": Füge " + v._1._2.get().size()
										+ " Kassendienste hinzu");
							}
							// wischer
							if (v._2.isPresent()) {
								v._1._1._1.getDienste().addAll(v._2.get());
								logger.debug("Spieltag " + tag + ": Füge " + v._2.get().size()
										+ " Wischerdienste hinzu");
							}
							logger.debug("Anzahl Dienste am Spieltag " + tag + ": " + v._1._1._1.getDienste().size());
							return new Tuple2<>(tag, v._1._1._1);
						});
		// System.out.println("Dienste:");
		// dienste.foreach(d->System.out.println(d));

		/*
		 * Spielzeiten +- Puffer berechnen, um zu wissen welches Team wann keinen
		 * Arbeitsdienst haben sollte
		 */
		Map<Team, List<Person>> teams = new HashMap<>(
				personen.keyBy(p -> p.getTeam())
						.groupByKey()
						.mapValues(IterableUtil::toList)
						.collectAsMap());
		JavaPairRDD<HSGDate, Iterable<Tuple2<Team, HSGInterval>>> spieltSelber =
				games	.filter(g -> teams.containsKey(g.getTeam()))
						.mapToPair(g -> new Tuple2<>(
								g.getDate(),
								new Tuple2<>(g.getTeam(), g.getDienstSperrenZeitraum())))
						.groupByKey();

		// System.out.println("Eigene Spielzeit:");
		// spieltSelber.foreach(r -> System.out.println(r));

		JavaPairRDD<HSGDate, Spieltag> spieltage =
				dienste	.leftOuterJoin(spieltSelber)
						.leftOuterJoin(blockierteTrainer)
						.mapValues(v -> {
							Spieltag st = v._1._1;
							if (v._1._2.isPresent()) {
								st.getAuswärtsSpielZeiten().addAll(IterableUtil.toList(v._1._2.get()));
							}
							if (v._2.isPresent()) {
								st.getBlockiertePersonen().addAll(IterableUtil.toList(v._2.get()));
							}
							st.dienste.forEach(d -> d.setDatum(st.datum));
							return st;
						});
		// System.out.println("Spieltage mit eigenen (parallelen) Spielen:");
		// spieltage.foreach(s -> System.out.println(s));
		return spieltage.cache();
	}

	private static List<Dienst> erstelleElternDienste(HSGDate date, Map<Team, HSGInterval> elternVerkaufsZeiten) {
		return elternVerkaufsZeiten	.entrySet()
									.stream()
									.map(t -> new Dienst(date, t.getValue(), Typ.Verkauf, t.getKey()))
									.collect(Collectors.toList());
	}

	private static Spielzeiten berechneDienstzeiten(Iterable<Game> gamesIt) {
		List<Game> games = IterableUtil.toList(gamesIt);
		games.sort(Game::compareTo);

		Spielzeiten sz = new Spielzeiten();
		// Die Gesamtzeit ist bei F1 eine Halbe
		sz.setGesamtZeit(new HSGInterval(games.get(0).getTeam() == Team.F1
				? games.get(0).getZeit().minusMinutes(F1_EARLY_START_MINUTES)
				: games.get(0).getZeit(),
				games.get(games.size() - 1).getZeit()));

		/*
		 * Elternverkaufszeit: Hier gibt es eine explizite Logik für die Dienste
		 * "rund um" die Spiele der C,D,E,F-Jugend
		 */
		List<Game> elternGames = games	.stream()
										.filter(g -> HSGApp.ELTERN_DIENSTE.contains(g.getTeam()))
										.sorted()
										.collect(Collectors.toList());
		Map<Team, HSGInterval> elternDienste = new HashMap<>();
		LocalTime current = null;
		if (!elternGames.isEmpty()) {
			current = elternGames.get(0).getZeit().minusMinutes(30);
			for (Game g : elternGames) {
				LocalTime end = g.getZeit().plusMinutes(90);
				elternDienste.put(g.getTeam(), new HSGInterval(current, end));
				current = end;
			}
		}
		sz.setElternVerkaufsZeiten(elternDienste);

		/*
		 * Spielerverkaufszeit: Reguläres vorgehen wie bisher.
		 */
		List<Game> spielerGames = games	.stream()
										.filter(g -> !HSGApp.ELTERN_DIENSTE.contains(g.getTeam()))
										.sorted()
										.collect(Collectors.toList());

		/*
		 * Falls wir eine aktuelle endzeit der elternverkaufszeit haben, startet der
		 * spielerverkaufszeitraum Typ.Verkauf.getVorlaufHS() halbe stunden danach (=der
		 * vorlauf, der später wieder runtergerechnet wird)
		 * Hier gilt "elternzeit first", d.h. die eltern arbeiten immer bis 90 min nach
		 * dem spiel ihrer kleinen, und der spielerverkaufsdienst beginnt danach.
		 */
		LocalTime sstart = current != null ? current.plusMinutes(Typ.Verkauf.getVorlaufHS() * 30)
				: spielerGames.get(0).getZeit();
		HSGInterval spielerVerkaufsZeit = spielerGames.isEmpty() ? HSGInterval.EMPTY
				: new HSGInterval(sstart, spielerGames.get(spielerGames.size() - 1).getZeit());

		sz.setSpielerVerkaufsZeit(spielerVerkaufsZeit);

		logger.debug(games.get(0).getDate() + " Spielzeiten: " + sz);

		return sz;
	}

	public static JavaRDD<Zuordnung> entferneDiensteAnNichtspieltagen(final JavaRDD<Game> games,
			JavaRDD<Zuordnung> zuordnungenAll, JavaPairRDD<HSGDate, Spieltag> spieltage) {
		JavaPairRDD<HSGDate, Iterable<Tuple2<HSGDate, Team>>> gamesByWE = games	.mapToPair(g -> new Tuple2<>(
				g.getDate().getWeekend(), new Tuple2<>(g.getDate(), g.getTeam())))
																				.groupByKey();

		logger.info("Zuordnungen vor Wochenendfilter: " + zuordnungenAll.count());
		// Experimentell: Alle Zurordnungen rauswerfen bei denen ein Spieler am jeweils
		// anderen Tag spielt
		JavaRDD<Zuordnung> zuordnungen =
				zuordnungenAll	.groupBy(z -> z.getDienst().getDatum().getWeekend())
								.leftOuterJoin(gamesByWE)
								.flatMap(d -> {
									// Kein Filtern wenn keine
									if (d._1 == null || !d._2._2.isPresent()) {
										return d._2._1.iterator();
									}
									// List<Zuordnung> l = IterableUtil.toList(d._2._1);
									final Set<Team> sa =
											StreamSupport	.stream(d._2._2.get().spliterator(), false)
															.filter(t -> t._1.isSaturday())
															.map(t -> t._2)
															.collect(Collectors.toSet());
									final Set<Team> so =
											StreamSupport	.stream(d._2._2.get().spliterator(), false)
															.filter(t -> t._1.isSunday())
															.map(t -> t._2)
															.collect(Collectors.toSet());
									if (sa.isEmpty() || so.isEmpty()) {
										return d._2._1.iterator();
									}

									List<Zuordnung> res = new ArrayList<>();
									for (Zuordnung z : d._2._1) {
										if (z.getPerson().getTeam() != null && z.getDienst().getDatum().isSaturday()
												&& so.contains(z.getPerson().getTeam())) {
											logger.debug(z.getPerson() + " wird am Samstag den "
													+ z.getDienst().getDatum()
													+ " nicht arbeiten weil er/sie Sonntag spielt.");
											continue;
										}
										if (z.getPerson().getTeam() != null && z.getDienst().getDatum().isSunday()
												&& sa.contains(z.getPerson().getTeam())) {
											logger.debug(z.getPerson() + " wird am Sonntag den "
													+ z.getDienst().getDatum()
													+ " nicht arbeiten weil er/sie Samstag spielt.");
											continue;
										}
										res.add(z);
									}
									return res.iterator();
								})
								.cache();
		final List<Dienst> missing = checkAlleNotwendigenZuordnungenSindVorhanden(spieltage, zuordnungen);
		JavaRDD<Zuordnung> reAdded = zuordnungenAll.filter(z -> missing.contains(z.getDienst()));
		logger.warn("Zuviel entfernte Zuordnungen (wieder hinzugefügt): " + reAdded.count());
		zuordnungen = zuordnungen.union(reAdded);
		logger.info("Zuordnungen nach Wochenendfilter: " + zuordnungen.count());

		return zuordnungen.cache();
	}

	/**
	 * TODO Method description.
	 *
	 * @param spieltage
	 * @param zuordnungen
	 */
	private static List<Dienst> checkAlleNotwendigenZuordnungenSindVorhanden(
			final JavaPairRDD<HSGDate, Spieltag> spieltage, final JavaRDD<Zuordnung> zuordnungen) {
		List<Dienst> ohneZuordnung =
				spieltage	.flatMap(s -> s._2.getDienste().iterator())
							.filter(d -> !d.isElternVerkaufsDienst()) // Nur die nicht schon fixierten
																		// elternverkaufsdienste müssen
																		// zugewiesen werden.
							.distinct()
							.keyBy(d -> d)
							.leftOuterJoin(zuordnungen.map(z -> z.getDienst()).distinct().keyBy(d -> d))
							.filter(t -> !t._2._2.isPresent())
							.keys()
							.collect();
		ohneZuordnung.forEach(d -> logger.error(d + ": Keine Personen zugeordnet"));
		return ohneZuordnung;
	}

	/**
	 * TODO Method description.
	 *
	 * @param personen
	 * @param spieltage
	 * @return
	 */
	private static JavaRDD<Zuordnung> erzeugeZuordnungen(final JavaRDD<Person> personen,
			final JavaPairRDD<HSGDate, Spieltag> spieltage) {
		final List<Person> personenList = new ArrayList<>(personen.collect());
		JavaRDD<Zuordnung> zuordnungen = spieltage.flatMap(t -> {
			List<Zuordnung> res = new ArrayList<>();
			for (Dienst d : t._2.getDienste()) {

				/*
				 * Für einen Dienst an dem die Eltern tätig sind, kann hier direkt der
				 * Platzhalter der Eltern genommen werden.
				 */
				if (d.isElternVerkaufsDienst()) {
					for (int i = 0; i < d.getTyp().getPersonen(); i++) {
						Zuordnung z = new Zuordnung(d.getParentsOf().getEltern(), d, i);
						z.setFixed(true);
						res.add(z);
					}
					continue;
				}

				Set<Team> playsConcurrently =
						t._2.getAuswärtsSpielZeiten()
							.stream()
							.filter(v -> v._2.intersects(d.zeit))
							.map(v -> v._1)
							.collect(Collectors.toSet());
				Set<Person> personIsBlocked =
						t._2.getBlockiertePersonen()
							.stream()
							.filter(v -> v._2.intersects(d.zeit))
							.map(v -> v._1)
							.collect(Collectors.toSet());
				for (Person p : personenList) {
					/*
					 * Nur Zuordnungen erlauben, deren: - Team nicht gerade selber spielt - Person
					 * in der Schicht auch arbeiten darf.
					 */
					if (!p.mayWorkAt(d)) {
						logger.debug(p + " darf generell nicht in Dienst " + d + " Arbeiten");
						continue;
					}
					if (playsConcurrently.contains(p.getTeam())) {
						logger.debug(p.getTeam() + "(" + p + ") kann wegen eigener Spiele nicht in Dienst " + d
								+ " arbeiten.");
						continue;
					}
					if (personIsBlocked.contains(p)) {
						logger.info(p + " kann als Trainer wegen eigener Spiele nicht in Dienst " + d + " arbeiten.");
						continue;
					}
					// So viele Zuordnungen wie Personen im Dienst hinzufügen
					for (int i = 0; i < (TEAMS_ONLY ? 1 : d.getTyp().getPersonen()); i++) {
						res.add(new Zuordnung(p, d, i));
					}
				}
			}
			return res.iterator();
		}).cache();
		// System.out.println("Zuordnungen:");
		// zuordnungen.foreach(z -> System.out.println(z.getPerson() + "@" +
		// z.getDienst() + "/" + z.getNr()));
		return zuordnungen;
	}

	private static HSGInterval berechneGesamtzeit(final HSGInterval minMax, final Typ typ) {
		return new HSGInterval(minMax.getStart().minus(typ.getVorlaufHS() * 30, ChronoUnit.MINUTES),
				minMax.getEnd().plus(typ.getNachlaufHS() * 30, ChronoUnit.MINUTES));
	}

	public static List<Dienst> verteileDienste(final HSGInterval range, final Dienst.Typ typ) {
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

	public static int optimaleDienstlänge(final LocalTime start, final LocalTime end, final Typ typ) {
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

			convertToISO(file);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
