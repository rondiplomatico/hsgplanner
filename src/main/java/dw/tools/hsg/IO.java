package dw.tools.hsg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Charsets;

import dw.tools.hsg.data.Dienst;
import dw.tools.hsg.data.Game;
import dw.tools.hsg.data.HSGDate;
import dw.tools.hsg.data.Person;
import dw.tools.hsg.data.Zuordnung;
import dw.tools.hsg.data.Dienst.Typ;
import dw.tools.hsg.util.IterableUtil;
import scala.Tuple2;

public class IO {

	public static Logger logger = Logger.getLogger(IO.class);

	public static final HSGDate START_DATE = null; //new HSGDate(2022, 9, 20);
	public static final HSGDate END_DATE = null; // new HSGDate(2119, 12, 31); // HSGDate.TODAY.nextDay(1);
	/**
	 * Anzahl der Spalten, in denen im HelferlisteHallendienste Helfer eingetragen
	 * werden können. Dies ist mit 6 mehr als die max 2 Personen in Diensten,
	 * ermöglicht aber Copy&Paste des Ergebnis-CSVs in Google Sheets mit korrekter
	 * Spaltenreferenzierung für die Auswertesheets Ü19 etc
	 */
	private static final int ANZ_HELFER_SPALTEN = 6;

	public static JavaRDD<Game> ladeSpiele(JavaSparkContext jsc, Path spieleCSVRaw) {
		Path spieleCSV = prepare(spieleCSVRaw, true);
		JavaRDD<Game> games = jsc
			.textFile(spieleCSV.toString())
			.map(Game::parse)
			.filter(g -> g != null
				&& (START_DATE == null || g.getDate().after(START_DATE))
				&& (END_DATE == null || g.getDate().before(END_DATE)))
			// .sortBy(g -> g, true, 1)
			// .repartition(6)
			.cache();
		games.filter(g -> g.isHeimspiel())
			.foreach(g -> logger.info("Spiel:" + g));
		logger.info("Importierte Spiele: " + games.count() + ", davon Heimspiele: "
			+ games.filter(g -> g.isHeimspiel()).count());
		return games;
	}

	public static JavaRDD<Person> ladePersonen(JavaSparkContext jsc, Path spielerCSVRaw) {
		Path spielerCSV = prepare(spielerCSVRaw, false);
		JavaRDD<Person> personen = jsc.textFile(spielerCSV.toString())
			.map(Person::parse)
			.filter(p -> p != null)
			// Remove any person that may not work
			.filter(Person::mayWork)
			.cache();
		personen.foreach(p -> logger.debug("Person " + p));
		return personen;
	}

	public static JavaRDD<Zuordnung> ladeZuordnungen(JavaSparkContext jsc, Path zuordnungenRaw) {
		return jsc.textFile(zuordnungenRaw.toString())
			.flatMap(l -> Zuordnung.read(l).iterator());
	}
	
	public static JavaRDD<String> toCSV(final JavaRDD<Game> games, final JavaRDD<Zuordnung> zu) {
		return zu.keyBy(z -> z.getDienst().getDatum()).groupByKey().mapValues(t -> {
			List<Tuple2<Dienst, List<Zuordnung>>> hlp
				= new ArrayList<>(StreamSupport
					.stream(t.spliterator(), false)
					.collect(Collectors.groupingBy(z -> z.getDienst()))
					.entrySet()
					.stream()
					.map(e -> new Tuple2<>(e.getKey(), e.getValue()))
					.collect(Collectors.toList()));
			Collections.sort(hlp, (a, b) -> a._1.compareTo(b._1));
			return hlp;
		}).join(games.keyBy(g -> g.getDate()).groupByKey().mapValues(v -> {
			List<Game> res = IterableUtil.toList(v);
			Collections.sort(res);
			return res;
		})).sortByKey().flatMap(t -> {
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
					if (!HSGApp.TEAMS_ONLY || d._1.getTyp() == Typ.Aufsicht || d._1.isElternVerkaufsDienst()) {
						// Personenliste - immer 5 Personen (einheitliches Format in der Helferliste)
						d._2
							.subList(0, np)
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
					elems
						.add(HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM
							+ HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM
							+ HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
				}
				// Add 3 columns for time bookkeeping (";;" -> ";;;;"
				elems.add(HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
				// @@@@ Spiel
				// Entweder die Spieldaten, oder eine leere Zeile
				elems
					.add(i < t._2._2.size() ? t._2._2.get(i).toCSV()
						: HSGApp.CSV_DELIM + HSGApp.CSV_DELIM + HSGApp.CSV_DELIM);
				res.add(String.join(HSGApp.CSV_DELIM, elems));
			}
			return res.iterator();
		});
	}

	private static Path prepare(Path in, boolean convert) {
		Path res = new Path(in.getParent(), "utf8-" + in.getName());
		try {
			String str = new String(Files.readAllBytes(Paths.get(in.toString())), convert ? Charsets.ISO_8859_1
				: Charsets.UTF_8);
			str = str.replace(",", ";");
			Files
				.write(Paths.get(res.toString()),
					str
						.getBytes(Charsets.UTF_8),
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE,
					StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return res;
	}

	public static boolean saveAsFile(final JavaRDD<String> data, final Path file) {
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

	private static Path convertToISO(Path in) {
		Path res = new Path(in.getParent(), "iso-" + in.getName());
		try {
			String str = new String(Files.readAllBytes(Paths.get(in.toString())), Charsets.UTF_8);
			Files
				.write(Paths.get(res.toString()),
					str
						.getBytes(Charsets.ISO_8859_1),
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE,
					StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return res;
	}
}
