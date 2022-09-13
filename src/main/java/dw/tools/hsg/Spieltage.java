package dw.tools.hsg;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import dw.tools.hsg.data.Dienst;
import dw.tools.hsg.data.Dienst.Typ;
import dw.tools.hsg.data.Game;
import dw.tools.hsg.data.HSGDate;
import dw.tools.hsg.data.HSGInterval;
import dw.tools.hsg.data.Person;
import dw.tools.hsg.data.Spieltag;
import dw.tools.hsg.data.Team;
import dw.tools.hsg.util.IterableUtil;
import scala.Tuple2;
import scala.Tuple3;

public class Spieltage {

	public static Logger logger = Logger.getLogger(Spieltage.class);
	public static final DecimalFormat DEC_FORMAT = new DecimalFormat("##.##");

	public static JavaPairRDD<HSGDate, Spieltag> berechneSpieltage(JavaPairRDD<HSGDate, List<Dienst>> dienste, final JavaRDD<Game> games, final JavaRDD<Person> personen) {

		JavaPairRDD<HSGDate, Iterable<Tuple2<Person, HSGInterval>>> blockierteTrainer = berechneBlockierteTrainer(games, personen);

		JavaPairRDD<HSGDate, Iterable<Tuple2<Team, HSGInterval>>> spieltSelber = berechneEigeneSpielzeiten(games, personen);

		JavaPairRDD<HSGDate, Spieltag> spieltage = dienste.leftOuterJoin(spieltSelber)
			.leftOuterJoin(blockierteTrainer)
			.mapToPair(v -> {
				Spieltag st = new Spieltag();
				st.setDatum(v._1);
				st.setDienste(v._2._1._1);
				st.getDienste().forEach(d -> d.setDatum(st.getDatum()));
				if (v._2._1._2.isPresent()) {
					st.getAuswärtsSpielZeiten().addAll(IterableUtil.toList(v._2._1._2.get()));
				}
				if (v._2._2.isPresent()) {
					st.getBlockiertePersonen().addAll(IterableUtil.toList(v._2._2.get()));
				}
				return new Tuple2<>(v._1, st);
			});
		if (logger.isDebugEnabled()) {
			logger.debug("Spieltage mit eigenen (parallelen) Spielen:");
			spieltage.foreach(logger::debug);
		}
		return spieltage.cache();
	}

	private static JavaPairRDD<HSGDate, Iterable<Tuple2<Team, HSGInterval>>> berechneEigeneSpielzeiten(final JavaRDD<Game> games, final JavaRDD<Person> personen) {
		/*
		 * Spielzeiten +- Puffer berechnen, um zu wissen welches Team wann keinen
		 * Arbeitsdienst haben sollte
		 */
		HashSet<Team> teams = new HashSet<>(personen.map(Person::getTeam).distinct().collect());
		JavaPairRDD<HSGDate, Iterable<Tuple2<Team, HSGInterval>>> spieltSelber = games.filter(g -> teams.contains(g.getTeam()))
			.mapToPair(g -> new Tuple2<>(g.getDate(), new Tuple2<>(g.getTeam(), g.getDienstSperrenZeitraum())))
			.groupByKey();
		if (logger.isDebugEnabled()) {
			logger.debug("Eigene Spielzeit:");
			spieltSelber.foreach(logger::debug);
		}
		return spieltSelber;
	}

	private static JavaPairRDD<HSGDate, Iterable<Tuple2<Person, HSGInterval>>> berechneBlockierteTrainer(final JavaRDD<Game> games, final JavaRDD<Person> personen) {
		JavaPairRDD<HSGDate, Iterable<Tuple2<Person, HSGInterval>>> blockierteTrainer = personen
			.filter(t -> t.getTrainerVon() != null)
			.keyBy(Person::getTrainerVon)
			.join(games.keyBy(Game::getTeam))
			.mapToPair(t -> new Tuple2<>(t._2._2.getDate(), new Tuple2<>(t._2._1, t._2._2.getDienstSperrenZeitraum())))
			.groupByKey();
		if (logger.isDebugEnabled()) {
			logger.debug("Blockierte Trainer:");
			blockierteTrainer.foreach(logger::debug);
		}
		return blockierteTrainer;
	}

	public static Map<Team, Double> berechneZielarbeitszeiten(Map<Team, Tuple3<Integer, Double, Integer>> anzUndVorl, JavaPairRDD<HSGDate, Spieltag> spieltage) {

		final Map<Typ, Integer> zeitenNachTyp = new HashMap<>(spieltage
			.flatMap(s -> s._2.getDienste().iterator())
			.filter(d -> !d.isElternVerkaufsDienst())
			.distinct()
			.keyBy(d -> d.getTyp())
			.aggregateByKey(0,
				(ex, n) -> ex + n.getZeit().dauerInMin() * n.getTyp().getPersonen(),
				(a, b) -> a + b)
			.collectAsMap());
		zeitenNachTyp
			.forEach((t, z) -> logger.info("Gesamt zu leistende Zeit für " + t + ": " + z + " (" + DEC_FORMAT.format(z / 60.0) + "h, Eltern ausgeschlossen)"));

		final double gesamtVorleistung = anzUndVorl.values()
			.stream()
			.mapToDouble(v -> v._2())
			.sum();
		logger.info("Gesamtvorleistung " + gesamtVorleistung / 60.0 + "h");

		/*
		 * Zähle die Gesamtanzahl effektiver Personen (manche Teams haben einen
		 * Leistungsfaktor)
		 */
		double gesamtEffektivPersonen = anzUndVorl.entrySet()
			.stream()
			.filter(t -> !Team.Aufsicht.equals(t.getKey()))
			.mapToDouble(e -> getTeamEffektivPersonen(e.getKey(), e.getValue()))
			.sum();
		double vorleistung = anzUndVorl.entrySet()
			.stream()
			.filter(e -> !Team.Aufsicht.equals(e.getKey()))
			.mapToDouble(e -> e.getValue()._2())
			.sum();
		double planzeit = zeitenNachTyp.get(Typ.Kasse) + zeitenNachTyp.get(Typ.Verkauf) + zeitenNachTyp.get(Typ.Wischen);
		double zeitProPerson = (planzeit + vorleistung) / gesamtEffektivPersonen;
		logger.info("Arbeitszeit für " + gesamtEffektivPersonen
			+ " volle Personen an Kasse, Verkauf und Wischern " +
			DEC_FORMAT.format((planzeit + vorleistung) / 60) + "h [" + DEC_FORMAT.format(planzeit / 60) + "h plan, "
			+ DEC_FORMAT.format(vorleistung / 60) + "h vorgeleistet], im Schnitt "
			+ DEC_FORMAT.format(zeitProPerson)
			+ "min (" + DEC_FORMAT.format(zeitProPerson / 60.0) + "h) je Person");

		gesamtEffektivPersonen = anzUndVorl.get(Team.Aufsicht)._1();
		vorleistung = anzUndVorl.entrySet().stream().filter(e -> Team.Aufsicht.equals(e.getKey())).mapToDouble(e -> e.getValue()._2()).sum();
		planzeit = zeitenNachTyp.get(Typ.Aufsicht);
		double zeitProAufsicht = (planzeit + vorleistung) / gesamtEffektivPersonen;
		logger.info("Arbeitszeit für " + gesamtEffektivPersonen + " Aufsichtsmitglieder (inkl. "
			+ anzUndVorl.get(Team.Aufsicht)._3() + " Trainer) im Schnitt " +
			DEC_FORMAT.format((planzeit + vorleistung) / 60) + "h [" + DEC_FORMAT.format(planzeit / 60) + "h plan, "
			+ DEC_FORMAT.format(vorleistung / 60) + "h vorgeleistet], im Schnitt "
			+ DEC_FORMAT.format(zeitProAufsicht)
			+ "min (" + DEC_FORMAT.format(zeitProAufsicht / 60.0) + "h) je Person");

		Map<Team, Double> res = new HashMap<>(anzUndVorl.entrySet()
			.stream()
			.collect(Collectors.toMap(e -> e.getKey(), e -> {
				double gesamtZeit = getTeamEffektivPersonen(e.getKey(), e.getValue()) * (Team.Aufsicht.equals(e.getKey()) ? zeitProAufsicht : zeitProPerson);
				double schnittProSpieler = gesamtZeit / (double) e.getValue()._1();
				logger.info("Team " + e.getKey() + " muss die Saison "
					+ DEC_FORMAT.format(gesamtZeit / 60.0)
					+ "h arbeiten und hat schon "
					+ DEC_FORMAT.format(e.getValue()._2() / 60.0)
					+ "h geleistet. Bleiben "
					+ DEC_FORMAT.format((gesamtZeit - e.getValue()._2()) / 60.0)
					+ "h, macht "
					+ DEC_FORMAT.format(schnittProSpieler / 60.0)
					+ "h im Schnitt für "
					+ e.getValue()._1()
					+ " Spieler");
				/*
				 * Bei "nur Team" Berechnungen gibt es einen repräsentativen Spieler pro
				 * Team (ungleich Aufsicht). Der muss dann natürlich auch die Gesamtzeit
				 * des Teams leisten statt nur den Durchschnitt.
				 */
				return HSGApp.TEAMS_ONLY && !Team.Aufsicht.equals(e.getKey())
					? gesamtZeit
					: schnittProSpieler;
			})));
		return res;
	}

	/**
	 * Value _1 = Anzahl Spieler, Value_3 = Anzahl Trainer
	 * Trainer müssen nicht mitarbeiten, daher werden sie hier nicht eingerechnet.
	 * Trainer in der Aufsicht machen trotzdem vollen Aufsichtsdienst.
	 * 
	 * @param t
	 * @param anzUndVorl
	 * @return
	 */
	private static double getTeamEffektivPersonen(Team t, Tuple3<Integer, Double, Integer> anzUndVorl) {
		return t.getLeistungsFaktor() * (anzUndVorl._1() - (Team.Aufsicht.equals(t) ? 0 : anzUndVorl._3()));
	}
}
