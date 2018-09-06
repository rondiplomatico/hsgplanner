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
 * Anforderungen:
 * OK - Erstellung von Verkaufs- und Kassendienst-Einteilungen für alle Spieler aus allen Teams
 * OK - Keine Dienste 1 Stunde vorm/nach eigenen Heimspielen, 2 Stunden vor/nach eigenen Auswärtsspielen
 * - Kassendienste nur für Spiele von festlegbaren Teams, Beginn 1h vorher
 * - Anzahl der Helfer pro Dienst ist festlegbar, abhängig vom Typ (K/V) und Tageszeit
 * - Gesamtarbeitszeit pro Teammitglied ist ungefähr gleich über die Saison
 * - Berücksichtigung von schon geleisteten Arbeitszeiten vorher (Krautfest, EF, ..)
 * - Festlegbare Personen für Hallenaufsichten
 * - Arbeitszeit der Hallenaufsichten ist auch ca. gleich
 * - Hallenaufsichten können Teammitglieder sein
 * - Einstellbare Default-Arbeitszeiten (Verkauf, Kasse, Aufsicht)
 * - Auswertung der Arbeitszeit pro Team (aufgeschlüsselt nach Einsatz vor Saisonbeginn und Personenanzahl)
 * - Spieler und Hallenaufsichten können für beliebige Zeitintervalle nicht verfügbar sein
 * - Hallendienste am Besten vor eigenen Heimspielen
 * - Die Anzahl der Dienstzeit an Tagen ohne eigenes Heimspiel
 * - Festlegbare Initialstunden für Teams (gleichmäßige Verteilung auf Mitglieder)
 * - Aktive und Jugend werden berücksichtigt
 * - Keine Dienste bei Spielen von Jugendtrainern
 *
 */
public class HSGApp {

    public static final String GA = "4066";

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
        // option = new Option("f", "fields", true, "sets the field selection, separated by comma");
        // option.setRequired(false);
        // options.addOption(option);
        //
        // option = new Option("r", "repartition", true, "repartitions the parquet file");
        // option.setRequired(false);
        // options.addOption(option);
        //
        // option = new Option("ro", "repartition_object", true, "repartitions and object file");
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
        // Path out = new Path(in.getParent(), "converted" + (cmd.hasOption("f") || cmd.hasOption("a") ? ".csv" :
        // ".json"));
        // if (cmd.hasOption("o")) {
        // out = new Path(cmd.getOptionValue("o"));
        // }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("HSG Tool").set("spark.ui.enabled", "false")
                 .set("spark.eventLog.enabled", "false")
                 .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        final List<String> mitKasse = new ArrayList<>(Arrays.asList("F1", "F2", "M1"));

        Path in = new Path(args[1]);
        JavaRDD<Person> personen = jsc.textFile(in.toString())
                                      .map(Person::parse)
                                      .cache();

        HashMap<String, List<Person>> teams = new HashMap<>(personen.keyBy(p -> p.getTeamId())
                                                                    .groupByKey()
                                                                    .mapValues(IterableUtil::toList)
                                                                    .collectAsMap());
        // System.out.println(teams);

        in = new Path(args[0]);
        JavaRDD<Game> games = jsc.textFile(in.toString())
                                 .map(Game::parse)
                                 .filter(g -> HSGDate.TODAY.beforeOrEquals(g.getDate()))
                                 .cache();

        // System.out.println("Spiele:");
        // games.foreach(System.out::println);
        // Game(date=2018-11-17, zeit=12:00, halle=4066, heim=HSG Leinfelden-Echterdingen, teamId=wJC1, gast=TSV
        // Neuhausen/F. 1898 2, heimspiel=true, staffel=wJC-KLA)
        // Game(date=2018-11-24, zeit=10:45, halle=4035, heim=Rot-Weiss Neckar 2, teamId=wJC1, gast=HSG
        // Leinfelden-Echterdingen, heimspiel=false, staffel=wJC-KLA)
        // Game(date=2018-12-01, zeit=10:00, halle=4032, heim=TSV Neuhausen/F. 1898 2, teamId=wJC1, gast=HSG
        // Leinfelden-Echterdingen, heimspiel=false, staffel=wJC-KLA)

        JavaPairRDD<HSGDate, HSGInterval> spielZeiten =
                        games.filter(g -> g.isHeimspiel())
                             .keyBy(g -> g.getDate())
                             .aggregateByKey(HSGInterval.MAXMIN,
                                             (ex, g) -> ex.stretch(g.getZeit()),
                                             (a, b) -> a.merge(b));
        // System.out.println("Spielzeiten:");
        // spielZeiten.foreach(System.out::println);

        JavaPairRDD<HSGDate, Spieltag> verkauf =
                        spielZeiten.mapToPair(t -> {
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
                        games.filter(g -> g.isHeimspiel() && mitKasse.contains(g.getTeamId()))
                             .keyBy(g -> g.getDate())
                             .aggregateByKey(HSGInterval.MAXMIN,
                                             (ex, g) -> ex.stretch(g.getZeit()),
                                             (a, b) -> a.merge(b))
                             .mapValues(v -> berechneGesamtzeit(v, Typ.Kasse))
                             .mapValues(v -> verteileDienste(v, Typ.Kasse));
        // System.out.println("Spieltage mit Kasse:");
        // kassenZeiten.foreach(System.out::println);

        JavaPairRDD<HSGDate, Spieltag> dienste = verkauf.join(kassenZeiten)
                                                        .join(aufsichtsZeiten)
                                                        .mapValues(v -> {
                                                            v._1._1.getDienste().addAll(v._1._2);
                                                            v._1._1.getDienste().addAll(v._2);
                                                            return v._1._1;
                                                        });
        // System.out.println("Spieltage:");
        // (2019-01-26,Spieltag(datum=2019-01-26, erstesSpiel=10:00, letztesSpiel=20:00, dienste=[2x V 09:00 - 11:30, 2x
        // V 11:30 - 14:00, 2x V 14:00 - 16:30, 2x V 16:30 - 19:00, 2x V 19:00 - 22:00, 2x K 15:00 - 18:00, 2x K 18:00 -
        // 20:30, 1x A 09:00 - 13:30, 1x A 13:30 - 18:00, 1x A 18:00 - 23:00]))
        // dienste.foreach(System.out::println);

        /*
         * Spielzeiten +- Puffer berechnen, um zu wissen welches Team wann keinen Arbeitsdienst haben sollte
         */
        JavaPairRDD<HSGDate, Iterable<Tuple2<String, HSGInterval>>> spieltSelber =
                        games.filter(g -> teams.containsKey(g.getTeamId()))
                             .mapToPair(g -> {
                                 HSGInterval zeit = new HSGInterval(g.getZeit().minus(g.isHeimspiel() ? 1L : 2L, ChronoUnit.HOURS),
                                                                    g.getZeit().plus(g.isHeimspiel() ? 2L : 3L, ChronoUnit.HOURS));
                                 return new Tuple2<>(g.getDate(), new Tuple2<>(g.getTeamId(), zeit));
                             }).groupByKey();

        // System.out.println("Eigene Spielzeit:");
        // (2018-11-17,[(M1,19:00,21:00), (F1,17:00,19:00), (M3,14:15,18:15), (F2,15:00,17:00), (mJA1,13:00,17:00),
        // (mJB1,10:30,14:30), (wJB1,13:00,15:00)])
        // spieltSelber.foreach(System.out::println);

        JavaPairRDD<HSGDate, Tuple2<Spieltag, List<Tuple2<String, HSGInterval>>>> spieltage =
                        dienste.leftOuterJoin(spieltSelber)
                               .mapValues(v -> {
                                   List<Tuple2<String, HSGInterval>> owntimes = new ArrayList<>();
                                   if (v._2.isPresent()) {
                                       owntimes.addAll(IterableUtil.toList(v._2.get()));
                                   }
                                   Tuple2<Spieltag, List<Tuple2<String, HSGInterval>>> res = new Tuple2<>(v._1, owntimes);
                                   v._1.dienste.forEach(d -> d.setDatum(v._1.datum));
                                   return res;
                               });
        // System.out.println("Spieltage mit eigenen (parallelen) Spielen:");
        // (2018-09-08,(Spieltag(datum=2018-09-08, erstesSpiel=18:00, letztesSpiel=18:00, dienste=[2x V 17:00 - 20:00,
        // 2x K 17:00 - 18:30, 1x A 17:00 - 21:00]),[(F1,16:00 - 21:00), (M1,17:00 - 20:00)]))
        // spieltage.foreach(System.out::println);

        /*
         * TODO
         * - Aufsicht verschieden behandeln
         * - Zuordnungen für Jugendtrainer bei Auswärtsspielen vermeiden
         */
        JavaRDD<Zuordnung> zuordnungen =
                        spieltage.flatMap(t -> {
                            List<Zuordnung> res = new ArrayList<>();
                            for (Dienst d : t._2._1.dienste) {
                                List<String> forbidden = t._2._2.stream().filter(v -> v._2.intersects(d.zeit)).map(v -> v._1).collect(Collectors.toList());
                                for (Entry<String, List<Person>> e : teams.entrySet()) {
                                    if (!forbidden.contains(e.getKey())) {
                                        e.getValue().forEach(p -> {
                                            // So viele Zuordnungen wie Personen im Dienst hinzufügen
                                            for (int i = 0; i < d.getTyp().getPersonen(); i++) {
                                                res.add(new Zuordnung(p, d));
                                            }
                                        });
                                    }
                                }
                            }
                            return res.iterator();
                        }).cache();
        // System.out.println("Zuordnungen:");
        // zuordnungen.foreach(System.out::println);
        /*
         * (V 17:00 - 20:00,Person(name=Sophia Schmidtblaicher, teamId=F3, gearbeitetM=0))
         * (K 17:00 - 18:30,Person(name=Andreas St�hr, teamId=M2, gearbeitetM=300))
         */
        System.out.println("Zuordnungen gesamt: " + zuordnungen.count());

        // zuordnungen.mapToPair(z -> z.getDienst())

        HSGSolver.solve(zuordnungen);

        jsc.close();
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
         * Falls der letzte Dienst weniger als eine Stunde ist, den vorletzten um die Zeit verlängern
         * und den letzten rausnehmen.
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
