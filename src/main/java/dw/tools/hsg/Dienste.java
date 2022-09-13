package dw.tools.hsg;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;

import dw.tools.hsg.data.Dienst;
import dw.tools.hsg.data.Dienst.Typ;
import dw.tools.hsg.data.Game;
import dw.tools.hsg.data.HSGDate;
import dw.tools.hsg.data.HSGInterval;
import dw.tools.hsg.data.Spielzeiten;
import dw.tools.hsg.data.Team;
import dw.tools.hsg.util.IterableUtil;
import scala.Tuple2;

public class Dienste {

	public static Logger logger = Logger.getLogger(Dienste.class);

	public static JavaPairRDD<HSGDate, List<Dienst>> berechneDienste(final JavaRDD<Game> games) {
		/*
		 * Spielzeiten
		 */
		JavaPairRDD<HSGDate, Spielzeiten> spielZeiten = games.filter(g -> g.isHeimspiel())
			.groupBy(g -> g.getDate())
			.mapValues(t -> berechneDienstzeiten(t));
		// System.out.println("Spielzeiten:");
		// spielZeiten.foreach(s -> System.out.println(s));

		JavaPairRDD<HSGDate, List<Dienst>> verkauf = berechneVerkaufsdienste(spielZeiten);

		JavaPairRDD<HSGDate, List<Dienst>> aufsichtsZeiten = berechneAufsichtsDienste(spielZeiten);

		JavaPairRDD<HSGDate, List<Dienst>> kassenZeiten = berechneKassenDienste(games);

		JavaPairRDD<HSGDate, List<Dienst>> wischerDienst = berechneWischerdienste(games);

		/*
		 * Alle Dienste zusammenführen
		 */
		JavaPairRDD<HSGDate, List<Dienst>> dienste = verkauf.join(aufsichtsZeiten)
			.leftOuterJoin(kassenZeiten)
			.leftOuterJoin(wischerDienst)
			.mapToPair(t -> {
				HSGDate tag = t._1;
				Tuple2<Tuple2<Tuple2<List<Dienst>, List<Dienst>>, Optional<List<Dienst>>>, Optional<List<Dienst>>> v = t._2;

				// verkauf
				List<Dienst> res = v._1._1._1;

				// aufsicht
				res.addAll(v._1._1._2);
				logger.debug("Spieltag " + tag + ": Füge " + v._1._1._2.size() + " Aufsichtsdienste hinzu");

				// kassen
				if (v._1._2.isPresent()) {
					res.addAll(v._1._2.get());
					logger.debug("Spieltag " + tag + ": Füge " + v._1._2.get().size() + " Kassendienste hinzu");
				}

				// wischer
				if (v._2.isPresent()) {
					res.addAll(v._2.get());
					logger.debug("Spieltag " + tag + ": Füge " + v._2.get().size() + " Wischerdienste hinzu");
				}
				logger.debug("Anzahl Dienste am Spieltag " + tag + ": " + res.size());
				return new Tuple2<>(tag, res);
			});
		if (logger.isDebugEnabled()) {
			logger.debug("Dienste:");
			dienste.foreach(logger::debug);
		}
		return dienste;
	}

	private static JavaPairRDD<HSGDate, List<Dienst>> berechneAufsichtsDienste(JavaPairRDD<HSGDate, Spielzeiten> spielZeiten) {
		/*
		 * Aufsicht
		 */
		JavaPairRDD<HSGDate, List<Dienst>> aufsichtsZeiten
			= spielZeiten
				.mapValues(sz -> sz.getGesamtZeit())
				.mapValues(v -> berechneGesamtzeit(v, Typ.Aufsicht))
				.mapValues(v -> verteileDienste(v, Typ.Aufsicht));
		if (logger.isDebugEnabled()) {
			logger.debug("Aufsichtszeiten:");
			aufsichtsZeiten.foreach(logger::debug);
		}
		return aufsichtsZeiten;
	}

	private static JavaPairRDD<HSGDate, List<Dienst>> berechneVerkaufsdienste(JavaPairRDD<HSGDate, Spielzeiten> spielZeiten) {
		/*
		 * Verkauf
		 */
		JavaPairRDD<HSGDate, List<Dienst>> verkauf = spielZeiten.mapToPair(t -> {
			List<Dienst> res = new ArrayList<>();
			if (!HSGInterval.EMPTY.equals(t._2.getSpielerVerkaufsZeit())) {
				HSGInterval total = berechneGesamtzeit(t._2.getSpielerVerkaufsZeit(), Typ.Verkauf);
				res.addAll(verteileDienste(total, Typ.Verkauf));
			}
			if (!t._2.getElternVerkaufsZeiten().isEmpty()) {
				res.addAll(erstelleElternDienste(t._1, t._2.getElternVerkaufsZeiten()));
			}
			return new Tuple2<>(t._1, res);
		});
		if (logger.isDebugEnabled()) {
			logger.debug("Verkaufszeiten:");
			verkauf.foreach(logger::debug);
		}
		return verkauf;
	}

	private static JavaPairRDD<HSGDate, List<Dienst>> berechneKassenDienste(final JavaRDD<Game> games) {
		/*
		 * Kasse Sonderlocke F1: Der Start der Spielzeit wird dann künstlich eine halbe
		 * stunde nach vorne gelegt, damit die Kassenschicht früher anfängt.
		 */
		JavaPairRDD<HSGDate, List<Dienst>> kassenZeiten = games
			.filter(g -> g.isHeimspiel()
				&& g.getTeam().isMitKasse())
			.keyBy(g -> g.getDate())
			.aggregateByKey(HSGInterval.MAXMIN,
				(ex, g) -> g.getTeam().equals(Team.F1)
					&& g.getZeit().compareTo(ex.getStart()) <= 0
						? ex.stretch(g.getZeit().minusMinutes(HSGApp.F1_EARLY_START_MINUTES))
						: ex.stretch(g.getZeit()),
				(a, b) -> a.merge(b))
			.mapValues(v -> berechneGesamtzeit(v, Typ.Kasse))
			.mapValues(v -> verteileDienste(v, Typ.Kasse));
		if (logger.isDebugEnabled()) {
			logger.debug("Spieltage mit Kasse:");
			kassenZeiten.foreach(logger::debug);
		}
		return kassenZeiten;
	}

	private static JavaPairRDD<HSGDate, List<Dienst>> berechneWischerdienste(final JavaRDD<Game> games) {
		/*
		 * Wischer
		 */
		JavaPairRDD<HSGDate, List<Dienst>> wischerDienst = games.filter(g -> g.isHeimspiel() && g.getTeam().isMitWischer())
			.groupBy(g -> g.getDate())
			.mapValues(t -> {
				List<Game> wl = IterableUtil.toList(t);
				if (wl.size() == 1) {
					Game g = wl.get(0);
					LocalTime bis = g.getZeit().plus(60, ChronoUnit.MINUTES);
					HSGInterval time = new HSGInterval(g.getZeit(), bis);
					// Vor- Nachlauf einrechnen
					time = berechneGesamtzeit(time, Typ.Wischen);
					Dienst d = new Dienst(g.getDate(), time, Typ.Wischen, Team.None);
					return Collections.singletonList(d);
				}
				throw new RuntimeException("Zwei Wischerdienste pro Spieltag aktuell nicht implementiert.");
			});
		if (logger.isDebugEnabled()) {
			logger.debug("Wischerdienste:");
			wischerDienst.foreach(logger::debug);
		}
		return wischerDienst;
	}

	private static Spielzeiten berechneDienstzeiten(Iterable<Game> gamesIt) {
		List<Game> games = IterableUtil.toList(gamesIt);
		games.sort(Game::compareTo);

		Spielzeiten sz = new Spielzeiten();
		// Die Gesamtzeit ist bei F1 eine Halbe
		sz.setGesamtZeit(new HSGInterval(games.get(0).getTeam() == Team.F1
			? games.get(0).getZeit().minusMinutes(HSGApp.F1_EARLY_START_MINUTES)
			: games.get(0).getZeit(), games.get(games.size() - 1).getZeit()));

		/*
		 * Elternverkaufszeit: Hier gibt es eine explizite Logik für die Dienste
		 * "rund um" die Spiele der C,D,E,F-Jugend
		 */
		List<Game> elternGames = games.stream().filter(g -> HSGApp.ELTERN_DIENSTE.contains(g.getTeam())).sorted().collect(Collectors.toList());

		Map<Team, HSGInterval> elternDienste = new HashMap<>();
		LocalTime current = null;
		if (!elternGames.isEmpty()) {
			current = elternGames.get(0).getZeit().minusMinutes(Typ.Verkauf.getVorlaufMin());
			for (int i = 0; i < elternGames.size(); i++) {
				Game g = elternGames.get(i);
				Game next = i + 1 < elternGames.size() ? elternGames.get(i + 1) : null;
				LocalTime end = next != null ? next.getZeit().minusMinutes(30) : g.getZeit().plusMinutes(60);
				elternDienste.put(g.getTeam(), new HSGInterval(current, end));
				current = end;
			}
		}
		sz.setElternVerkaufsZeiten(elternDienste);

		/*
		 * Spielerverkaufszeit: Reguläres vorgehen wie bisher.
		 */
		List<Game> spielerGames = games.stream().filter(g -> !HSGApp.ELTERN_DIENSTE.contains(g.getTeam())).sorted().collect(Collectors.toList());

		/*
		 * Falls wir eine aktuelle endzeit der elternverkaufszeit haben, startet der
		 * spielerverkaufszeitraum Typ.Verkauf.getVorlaufHS() halbe stunden danach (=der
		 * vorlauf, der später wieder runtergerechnet wird)
		 * Hier gilt "elternzeit first", d.h. die eltern arbeiten immer bis 90 min nach
		 * dem spiel ihrer kleinen, und der spielerverkaufsdienst beginnt danach.
		 */
		LocalTime sstart = current != null ? current.plusMinutes(Typ.Verkauf.getVorlaufMin())
			: spielerGames.get(0).getZeit();
		HSGInterval spielerVerkaufsZeit = spielerGames.isEmpty() ? HSGInterval.EMPTY
			: new HSGInterval(sstart, spielerGames.get(spielerGames.size() - 1).getZeit());

		sz.setSpielerVerkaufsZeit(spielerVerkaufsZeit);

		logger.debug(games.get(0).getDate() + " Spielzeiten: " + sz);

		return sz;
	}

	static List<Dienst> verteileDienste(final HSGInterval range, final Dienst.Typ typ) {
		int durationInHalfHrs = optimaleDienstlänge(range.getStart(), range.getEnd(), typ);
		List<Dienst> dienste = new ArrayList<>();
		Dienst d = typ.newDienst();
		d.setTyp(typ);
		d.setZeit(new HSGInterval(range.getStart(), range.getStart().plus(30 * durationInHalfHrs, ChronoUnit.MINUTES)));
		dienste.add(d);
		while (d.getZeit().getEnd().isBefore(range.getEnd())) {
			d = typ.newDienst();
			LocalTime von = dienste.get(dienste.size() - 1).getZeit().getEnd();
			LocalTime bis = von.plus(30 * durationInHalfHrs, ChronoUnit.MINUTES);
			bis = bis.isAfter(range.getEnd()) || bis.isBefore(von) ? range.getEnd() : bis;
			d.setZeit(new HSGInterval(von, bis));
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

	static int optimaleDienstlänge(final LocalTime start, final LocalTime end, final Typ typ) {
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

	private static HSGInterval berechneGesamtzeit(final HSGInterval minMax, final Typ typ) {
		return new HSGInterval(minMax.getStart().minus(typ.getVorlaufMin(), ChronoUnit.MINUTES), minMax.getEnd()
			.plus(typ.getNachlaufMin(), ChronoUnit.MINUTES));
	}

	private static List<Dienst> erstelleElternDienste(HSGDate date, Map<Team, HSGInterval> elternVerkaufsZeiten) {
		return elternVerkaufsZeiten.entrySet().stream().map(t -> new Dienst(date, t.getValue(), Typ.Verkauf, t.getKey())).collect(Collectors.toList());
	}
}
