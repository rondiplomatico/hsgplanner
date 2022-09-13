package dw.tools.hsg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import dw.tools.hsg.data.Dienst;
import dw.tools.hsg.data.Person;
import dw.tools.hsg.data.Team;
import scala.Tuple2;
import scala.Tuple3;

public class Personen {

	public static Logger logger = Logger.getLogger(Personen.class);

	/**
	 * Bei nur Teambasierter Berechnung nehmen wir einfach einen representativen
	 * Spieler pro Team, der die Summe aller Teamvorarbeiten mitbekommt.
	 * Dies gilt nicht für die Aufsicht.
	 */
	public static JavaRDD<Person> reduziereAufTeamRepräsentanten(JavaRDD<Person> personen) {
		final Map<Team, Integer> vorleistungProTeam
			= new HashMap<>(personen.keyBy(p -> p.getTeam())
				.aggregateByKey(0,
					(ex, n) -> ex + n.getGearbeitetM(),
					(a, b) -> a + b)
				.collectAsMap());
		return personen.map(p -> p.isAufsicht() ? p : p.teamRepresentant())
			.distinct()
			.map(p -> {
				if (!p.isAufsicht()) {
					p.setGearbeitetM(vorleistungProTeam.get(p.getTeam()));
				}
				return p;
			})
			.cache();
	}

	/**
	 * Alle Spieler rauswerfen, die schon genug gearbeitet haben (reduziert die
	 * Problemkomplexität)
	 * 
	 * @param personen
	 * @param zielArbeitszeitProPersonJeTeam
	 * @return
	 */
	public static JavaRDD<Person> entferneDienstzeiterfüller(JavaRDD<Person> personen, final Map<Team, Double> zielArbeitszeitProPersonJeTeam) {
		int kürzesterDienst = Arrays.asList(Dienst.Typ.values()).stream().mapToInt(t -> t.getTimesHS()[0]).min().getAsInt()
			* 30;
		return personen.filter(p -> {
			boolean valid = p.getGearbeitetM() < zielArbeitszeitProPersonJeTeam.get(p.getTeam()) - kürzesterDienst / 2;
			if (!valid) {
				logger.info(p + " hat mit " + HSGApp.DEC_FORMAT.format(p.getGearbeitetM() / 60.0)
					+ "h mehr (oder ist hinreichend nah dran) als der erforderliche Durchschnitt von "
					+ HSGApp.DEC_FORMAT.format(zielArbeitszeitProPersonJeTeam.get(p.getTeam()) / 60.0)
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

	public static Map<Team, Tuple3<Integer, Double, Integer>> anzahlUndVorleistungJeTeam(JavaRDD<Person> personen) {
		final Map<Team, Tuple3<Integer, Double, Integer>> anzahlUndVorleistungProTeam = new HashMap<>(personen
			.keyBy(p -> p.getTeam())
			.aggregateByKey(new Tuple3<Integer, Double, Integer>(0, 0.0, 0),
				(ex, n) -> new Tuple3<>(ex._1() + 1, ex._2() + n.getGearbeitetM(), ex._3() + (n.getTrainerVon() != null ? 1 : 0)),
				(a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3() + b._3()))
			/*
			 * We assume each Jungendteam has at least MIN_TEAM_SIZE members (they might not
			 * be
			 * included in the import, as the team might not be assigned for especially
			 * youth teams.
			 */
			.mapToPair(t -> !t._1.isJugend() ? t : new Tuple2<>(t._1, new Tuple3<>(Math.max(t._2._1(), HSGApp.MIN_TEAM_SIZE), t._2._2(), t._2._3())))
			.collectAsMap());

		anzahlUndVorleistungProTeam.forEach((t, v) -> logger
			.info("Vorleistung von Team " + t + " mit " + v._1()
				+ " Spielern (davon " + v._3() + " Trainer): "
				+ v._2() + " (" + (v._2() / 60.0) + "h)"));
		return anzahlUndVorleistungProTeam;
	}

}
