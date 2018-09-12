package dw.tools.hsg;

import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
 *    - Die Dienstzeit an Tagen ohne eigenes Heimspiel ist minimal/es gibt keine
 *    - Festlegbare Initialstunden für Teams (gleichmäßige Verteilung auf Mitglieder)
 *    - Aufsicht und Dienste bevorzugt vor eigenen Heimspielen.
 *      Jede selber aktive Aufsichtsperson sollte "gleich viel Vorteil" haben.
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
 * - Nur festgelegte Teams haben Hallendienste.
 *   Alle Teams im Personenimport werden importiert, aber {@link Person#isValid()} prüft ob diese auch für
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
 * </pre>
 */
public class HSGApp {

    public static final String GA = "4066";
    public static final String CSV_DELIM = ";";
    private static Logger logger;

    static {
        ConsoleAppender console = new ConsoleAppender();
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.WARN);
        console.activateOptions();
        Logger.getRootLogger().addAppender(console);
        FileAppender fa = new FileAppender();
        fa.setFile("convert.log");
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setThreshold(Level.INFO);
        fa.setAppend(true);
        fa.activateOptions();
        Logger.getRootLogger().addAppender(fa);
        logger = Logger.getRootLogger();
    }

    public static void main(final String[] args) throws IOException, ParseException {
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
        sparkConf.setAppName("HSG Tool")
                 .set("spark.ui.enabled", "false")
                 .set("spark.eventLog.enabled", "false")
                 .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Path in = new Path(args[1]);
        JavaRDD<Person> personen = jsc.textFile(in.toString())
                                      // .map(s -> new String(s.getBytes(Charsets.ISO_8859_1), Charsets.UTF_8))
                                      .map(Person::parse)
                                      .filter(Person::isValid)
                                      .cache();

        in = new Path(args[0]);
        JavaRDD<Game> games = jsc.textFile(in.toString())
                                 // .map(s -> new String(s.getBytes(Charsets.ISO_8859_1), Charsets.UTF_8))
                                 .map(Game::parse)
                                 .filter(g -> HSGDate.TODAY.beforeOrEquals(g.getDate()))
                                 .cache();
        // System.out.println("Spiele:");
        // games.foreach(System.out::println);

        JavaRDD<Zuordnung> zu = jsc.parallelize(compute(games, personen));

        JavaRDD<String> content = toCSV(games, zu);
        Path out = new Path(in.getParent(), "dienste.csv");
        saveAsFile(content, out);

        exportStats(in, zu);

        jsc.close();

    }

    /**
     * @param in
     * @param zu
     * @param personen
     */
    private static void exportStats(final Path in, final JavaRDD<Zuordnung> zu) {
        JavaRDD<String> stats1 = zu.mapToPair(z -> new Tuple2<>(z.getPerson(), z.getDienst().getZeit().dauerInMin()))
                                   .groupByKey()
                                   .map(t -> {
                                       int effective = StreamSupport.stream(t._2.spliterator(), false).mapToInt(i -> i).sum();
                                       return String.join(HSGApp.CSV_DELIM, t._1.getName(), t._1.getTeam().toString(),
                                                          "" + t._1.getGearbeitetM(),
                                                          "" + effective,
                                                          Integer.toString(t._1.getGearbeitetM() + effective));
                                   });
        Map<Team, Integer> counts = new HashMap<>(zu.map(z -> z.getPerson()).distinct().mapToPair(p -> new Tuple2<>(p.getTeam(), 1))
                                                    .reduceByKey((a, b) -> a + b)
                                                    .collectAsMap());
        JavaRDD<String> stats2 = zu.mapToPair(z -> new Tuple2<>(z.getPerson().getTeam(),
                                                                new Tuple2<>(z.getPerson().getGearbeitetM(), z.getDienst().getZeit().dauerInMin())))
                                   .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                                   .map(t -> String.join(HSGApp.CSV_DELIM, "Gesamtsumme Team", t._1.toString(),
                                                         Integer.toString(t._2._1),
                                                         Integer.toString(t._2._2),
                                                         Integer.toString(t._2._1 + t._2._2),
                                                         Integer.toString(counts.get(t._1)),
                                                         Double.toString((t._2._1 + t._2._2) / (double) counts.get(t._1))));
        Path out = new Path(in.getParent(), "stats.csv");
        saveAsFile(stats1.union(stats2), out);
    }

    public static JavaRDD<String> toCSV(final JavaRDD<Game> games, final JavaRDD<Zuordnung> zu) {
        return zu.keyBy(z -> z.getDienst().getDatum())
                 .groupByKey().mapValues(t -> {
                     List<Tuple2<Dienst, List<Zuordnung>>> hlp =
                                     new ArrayList<>(StreamSupport.stream(t.spliterator(), false)
                                                                  .collect(Collectors.groupingBy(z -> z.getDienst()))
                                                                  .entrySet().stream().map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                                                                  .collect(Collectors.toList()));
                     Collections.sort(hlp, (a, b) -> a._1.compareTo(b._1));
                     return hlp;
                 })
                 .join(games.keyBy(g -> g.getDate()).groupByKey()
                            .mapValues(v -> {
                                List<Game> res = IterableUtil.toList(v);
                                Collections.sort(res);
                                return res;
                            }))
                 .sortByKey()
                 .flatMap(t -> {
                     // Spielinfo (4 spalten) Datum Von Bis Wo Was Anzahl Verantwortlich Helfer 1 Helfer 2 Helfer 3
                     List<String> res = new ArrayList<>();
                     int max = Math.max(t._2._1.size(), t._2._2.size());
                     for (int i = 0; i < max; i++) {
                         List<String> elems = new ArrayList<>(14);
                         elems.add(i < t._2._2.size() ? t._2._2.get(i).toCSV() : HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
                         elems.add(t._1.toddMMyyyy());
                         if (i < t._2._1.size()) {
                             Tuple2<Dienst, List<Zuordnung>> d = t._2._1.get(i);
                             elems.add(d._1.toCSV()); // 3 columns
                             elems.add(d._2.get(0).getPerson().getTeam().getId());
                             elems.add(Integer.toString(d._2.get(0).getDienst().getTyp().getPersonen()));
                             d._2.stream().map(z -> z.getPerson().getName()).forEach(elems::add);
                         }
                         res.add(String.join(HSGApp.CSV_DELIM, elems));
                     }
                     return res.iterator();
                 });
    }

    public static List<Zuordnung> compute(final JavaRDD<Game> games, final JavaRDD<Person> personen) {

        HashMap<Team, List<Person>> teams =
                        new HashMap<>(personen.keyBy(p -> p.getTeam())
                                              .groupByKey()
                                              .mapValues(IterableUtil::toList)
                                              .collectAsMap());
        // System.out.println(teams);
        JavaPairRDD<HSGDate, Iterable<Tuple2<Person, HSGInterval>>> blockierteTrainer =
                        personen.filter(t -> t.getTrainerVon() != null)
                                .keyBy(Person::getTrainerVon)
                                .join(games.keyBy(Game::getTeam))
                                .mapToPair(t -> new Tuple2<>(t._2._2.getDate(), new Tuple2<>(t._2._1, t._2._2.getDienstSperrenZeitraum())))
                                .groupByKey();
        // System.out.println("Blockierte Trainer:");
        // blockierteTrainer.foreach(System.out::println);

        JavaPairRDD<HSGDate, HSGInterval> spielZeiten =
                        games.filter(g -> g.isHeimspiel())
                             .keyBy(g -> g.getDate())
                             .aggregateByKey(HSGInterval.MAXMIN,
                                             (ex, g) -> ex.stretch(g.getZeit()),
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

        JavaPairRDD<HSGDate, List<Dienst>> aufsichtsZeiten =
                        spielZeiten.mapValues(v -> berechneGesamtzeit(v, Typ.Aufsicht))
                                   .mapValues(v -> verteileDienste(v, Typ.Aufsicht));
        // System.out.println("Aufsichtszeiten:");
        // aufsichtsZeiten.foreach(System.out::println);

        JavaPairRDD<HSGDate, List<Dienst>> kassenZeiten =
                        games.filter(g -> g.isHeimspiel() && g.getTeam().mitKasse())
                             .keyBy(g -> g.getDate())
                             .aggregateByKey(HSGInterval.MAXMIN,
                                             (ex, g) -> ex.stretch(g.getZeit()),
                                             (a, b) -> a.merge(b))
                             .mapValues(v -> berechneGesamtzeit(v, Typ.Kasse))
                             .mapValues(v -> verteileDienste(v, Typ.Kasse));
        // System.out.println("Spieltage mit Kasse:");
        // kassenZeiten.foreach(System.out::println);

        JavaPairRDD<HSGDate, Spieltag> dienste =
                        verkauf.join(kassenZeiten)
                               .join(aufsichtsZeiten)
                               .mapValues(v -> {
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
        JavaPairRDD<HSGDate, Iterable<Tuple2<Team, HSGInterval>>> spieltSelber =
                        games.filter(g -> teams.containsKey(g.getTeam()))
                             .mapToPair(g -> new Tuple2<>(g.getDate(), new Tuple2<>(g.getTeam(), g.getDienstSperrenZeitraum())))
                             .groupByKey();

        // System.out.println("Eigene Spielzeit:");
        // spieltSelber.foreach(r -> System.out.println(r));

        JavaPairRDD<HSGDate, Spieltag> spieltage =
                        berechneSpieltage(dienste, spieltSelber, blockierteTrainer);

        JavaRDD<Zuordnung> zuordnungen = erzeugeZuordnungen(personen, spieltage);

        checkAlleNotwendigenZuordnungenSindVorhanden(spieltage, zuordnungen);

        return HSGSolver.solve(zuordnungen, games);
    }

    /**
     * TODO Method description.
     *
     * @param dienste
     * @param spieltSelber
     * @param blockierteTrainer
     * @return
     */
    private static JavaPairRDD<HSGDate, Spieltag> berechneSpieltage(final JavaPairRDD<HSGDate, Spieltag> dienste,
                    final JavaPairRDD<HSGDate, Iterable<Tuple2<Team, HSGInterval>>> spieltSelber,
                    final JavaPairRDD<HSGDate, Iterable<Tuple2<Person, HSGInterval>>> blockierteTrainer) {
        JavaPairRDD<HSGDate, Spieltag> spieltage =
                        dienste.leftOuterJoin(spieltSelber)
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
        return spieltage;
    }

    /**
     * TODO Method description.
     *
     * @param spieltage
     * @param zuordnungen
     */
    private static void checkAlleNotwendigenZuordnungenSindVorhanden(final JavaPairRDD<HSGDate, Spieltag> spieltage,
                    final JavaRDD<Zuordnung> zuordnungen) {
        List<Dienst> ohneZuordnung = spieltage.flatMap(s -> s._2.getDienste().iterator())
                                              .distinct()
                                              .keyBy(d -> d)
                                              .leftOuterJoin(zuordnungen.map(z -> z.getDienst())
                                                                        .distinct()
                                                                        .keyBy(d -> d))
                                              .filter(t -> !t._2._2.isPresent())
                                              .keys()
                                              .collect();
        if (!ohneZuordnung.isEmpty()) {
            ohneZuordnung.forEach(d -> logger.error(d + ": Keine Personen zugeordnet"));
            throw new RuntimeException("Für " + ohneZuordnung.size() + " Dienste konnten keine möglichen Personen zugeordnet werden.");
        } else {
            logger.info("Zuordnungen gesamt: " + zuordnungen.count());
        }
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
                Set<Team> playsConcurrently = t._2.getAuswärtsSpielZeiten().stream()
                                                  .filter(v -> v._2.intersects(d.zeit))
                                                  .map(v -> v._1)
                                                  .collect(Collectors.toSet());
                Set<Person> personIsBlocked = t._2.getBlockiertePersonen().stream()
                                                  .filter(v -> v._2.intersects(d.zeit))
                                                  .map(v -> v._1)
                                                  .collect(Collectors.toSet());
                for (Person p : personenList) {
                    /*
                     * Nur Zuordnungen erlauben, deren:
                     * - Team nicht gerade selber spielt
                     * - Person in der Schicht auch arbeiten darf.
                     */
                    if (!p.mayWorkAt(d)) {
                        logger.warn(p + " darf generell nicht in Dienst " + d + " Arbeiten");
                        continue;
                    }
                    if (playsConcurrently.contains(p.getTeam())) {
                        logger.warn(p.getTeam() + "(" + p + ") kann wegen eigener Spiele nicht in Dienst " + d + " arbeiten.");
                        continue;
                    }
                    if (personIsBlocked.contains(p)) {
                        logger.warn(p + " kann als Trainer wegen eigener Spiele nicht in Dienst " + d + " arbeiten.");
                        continue;
                    }
                    // So viele Zuordnungen wie Personen im Dienst hinzufügen
                    for (int i = 0; i < d.getTyp().getPersonen(); i++) {
                        res.add(new Zuordnung(p, d, i));
                    }
                }
            }
            return res.iterator();
        }).cache();
        // System.out.println("Zuordnungen:");
        // zuordnungen.foreach(z -> System.out.println(z.getPerson() + "@" + z.getDienst() + "/" + z.getNr()));
        return zuordnungen;
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
