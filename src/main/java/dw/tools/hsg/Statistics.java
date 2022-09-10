package dw.tools.hsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Lists;

import dw.tools.hsg.data.Person;
import dw.tools.hsg.data.Team;
import dw.tools.hsg.data.Zuordnung;
import scala.Tuple2;

public class Statistics {

	public static Logger logger = Logger.getLogger(Statistics.class);

	public static void exportStats(JavaSparkContext jsc, final Path out, final JavaRDD<Zuordnung> zu, final JavaRDD<Person> personen, Map<Team, Double> zielArbeitszeitProPersonJeTeam) {
		JavaPairRDD<Person, Integer> effectiveWorkTime
			= zu.mapToPair(
				/**
				 * ACHTUNG!! TEAMS_ONLY_EXCEPTION
				 * Bei TEAMS ONLY gibt es nur einen Spieler pro Team und daher auch nur einen
				 * Slot, der bei der Optimierung belegt werden kann. Daher wird hier auch nur
				 * eine Zuordnung pro Dienst
				 * vorgenommen. (Es macht keinen Sinn, künstlich 2 Spieler anzulegen oder wie
				 * viele
				 * auch immer der "größte" dienst braucht). Da aber sonst die Gesamtzeiten für
				 * ein Team natürlich die Anzahl der gleichzeitig Arbeitenden Personen
				 * berücksichtigt, gibt es andere Stellen, an denen dieser Ausnahmekontext
				 * gilt:
				 * HSGSolver#addZ1GleichVielArbeitsZeit
				 * Zuordnungen#erzeugeZuordnungen
				 */
				z -> new Tuple2<>(z.getPerson(), z.getDienst().getZeit().dauerInMin() * (HSGApp.TEAMS_ONLY ? z.getDienst().getTyp().getPersonen() : 1)))
				.reduceByKey((a, b) -> a + b)
				.cache();
		effectiveWorkTime.foreach(t -> logger.info("Effektive Arbeitszeit von " + t._1 + ": " + t._2));
		final Tuple2<Integer, Integer> avgAufsichtT = effectiveWorkTime.filter(z -> z._1.isAufsicht()).isEmpty()
			? new Tuple2<>(0, 1)
			: effectiveWorkTime
				.filter(z -> z._1.isAufsicht())
				.map(z -> new Tuple2<>(z._2, 1))
				.reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2
					+ b._2));
		final Tuple2<Integer, Integer> avgDienstT = effectiveWorkTime.filter(z -> !z._1.isAufsicht()).isEmpty()
			? new Tuple2<>(0, 1)
			: effectiveWorkTime
				.filter(z -> !z._1.isAufsicht())
				.map(z -> new Tuple2<>(z._2, 1))
				.reduce((a, b) -> new Tuple2<>(a._1 + b._1, a._2
					+ b._2));
		Map<Team, Integer> teamGröße
			= new HashMap<>(zu
				.map(z -> z.getPerson())
				.filter(p -> !p.isAufsicht())
				.distinct()
				.mapToPair(p -> new Tuple2<>(p.getTeam(), 1))
				.reduceByKey((a, b) -> a
					+ b)
				.collectAsMap());
		final double avgAufsicht = avgAufsichtT._1 / (double) avgAufsichtT._2;
		final double avgDienst = avgDienstT._1 / (double) avgDienstT._2;
		logger.warn("Durchschn. Arbeitszeit Normal: " + avgDienst + ", Aufsicht:" + avgAufsicht);

		/*
		 * Zeiten pro Person
		 */
		JavaRDD<String> stats1 = effectiveWorkTime.map(t -> {
			int total = t._1.getGearbeitetM() + t._2;

			List<String> cols = new ArrayList<>();
			// Name
			cols.add(t._1.getName() + (t._1.isAufsicht() ? " (A)" : ""));
			// Team
			cols.add(t._1.getTeam().toString());
			// Gearbeitet
			cols.add(Integer.toString(t._1.getGearbeitetM()));
			// Geplant
			cols.add(Integer.toString(t._2));
			// Gesamt
			cols.add(Integer.toString(total));
			// Sollzeit
			cols.add(Double.toString(zielArbeitszeitProPersonJeTeam.getOrDefault(t._1.getTeam(), 0.0)).replace('.', ','));
			// Abweichung sollzeit
			cols.add(Double.toString(total - (zielArbeitszeitProPersonJeTeam.getOrDefault(t._1.getTeam(), (double) total))).replace('.', ','));
			// Abweichung effektiver Durchschnitt
			cols.add(Double.toString(total - (t._1.isAufsicht() ? avgAufsicht : avgDienst)).replace('.', ','));
			return String.join(HSGApp.CSV_DELIM, cols);
		}).sortBy(s -> s, true, 1);
		stats1 = jsc.parallelize(Lists.newArrayList("Name;Team;Gearbeitet;Geplant;Gesamt;Sollzeit;Abweichung Sollzeit;Abweichung effektiver Durchschnitt"))
			.union(stats1);

		/*
		 * Zeiten pro Team
		 */
		JavaRDD<String> stats2
			= effectiveWorkTime
				.filter(z -> !z._1.isAufsicht())
				.mapToPair(z -> new Tuple2<>(z._1.getTeam(), new Tuple2<>(z._1.getGearbeitetM(), z._2)))
				.reduceByKey((a, b) -> new Tuple2<>(a._1
					+ b._1, a._2
						+ b._2))
				.map(t -> String.join(HSGApp.CSV_DELIM,
					"Gesamtsumme Team",
					t._1.toString(),
					Integer.toString(t._2._1),
					Integer.toString(t._2._2),
					Integer.toString(t._2._1
						+ t._2._2),
					Integer.toString(teamGröße.get(t._1)),
					Double.toString((t._2._1 + t._2._2) / (double) teamGröße.get(t._1)).replace('.', ',')))
				.sortBy(s -> s, true, 1);
		;

		/*
		 * Personen ohne Arbeitsdienste
		 */
		JavaRDD<String> stats3 = personen.keyBy(p -> p).leftOuterJoin(zu.mapToPair(z -> new Tuple2<>(z.getPerson(), 1))).map(d -> {
			if (!d._2._2.isPresent()) {
				return String.join(HSGApp.CSV_DELIM,
					"Kein Dienst",
					d._1.getName() + (d._1.isAufsicht() ? " (A)" : ""),
					d._1.getTeam().toString(),
					Integer.toString(d._1.getGearbeitetM()),
					Double.toString(d._1.getGearbeitetM() - (d._1.isAufsicht() ? avgAufsicht : avgDienst)).replace('.', ','));
			} else {
				return null;
			}
		}).filter(s -> s != null).sortBy(s -> s, true, 1);

		IO.saveAsFile(stats1.union(stats2).union(stats3), new Path(out, "stats.csv"));
	}
}
