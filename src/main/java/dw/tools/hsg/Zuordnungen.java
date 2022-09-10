package dw.tools.hsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import dw.tools.hsg.data.Dienst;
import dw.tools.hsg.data.Game;
import dw.tools.hsg.data.HSGDate;
import dw.tools.hsg.data.Person;
import dw.tools.hsg.data.Spieltag;
import dw.tools.hsg.data.Team;
import dw.tools.hsg.data.Zuordnung;
import scala.Tuple2;
import scala.Tuple3;

public class Zuordnungen {

	public static Logger logger = Logger.getLogger(Zuordnungen.class);

	/**
	 *
	 * @param personen
	 * @param spieltage
	 * @return
	 */
	public static JavaRDD<Zuordnung> erzeugeZuordnungen(final JavaRDD<Person> personen, final JavaPairRDD<HSGDate, Spieltag> spieltage) {
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

				Set<Team> playsConcurrently = t._2.getAuswärtsSpielZeiten()
					.stream()
					.filter(v -> v._2.intersects(d.getZeit()))
					.map(v -> v._1)
					.collect(Collectors.toSet());
				Set<Person> personIsBlocked = t._2.getBlockiertePersonen()
					.stream()
					.filter(v -> v._2.intersects(d.getZeit()))
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
					/**
					 * ACHTUNG!! TEAMS_ONLY_EXCEPTION
					 * Bei TEAMS ONLY gibt es nur einen Spieler pro Team und daher auch nur einen
					 * Slot, der bei der Optimierung belegt werden kann. Daher wird hier auch nur eine Zuordnung pro Dienst
					 * vorgenommen. (Es macht keinen Sinn, künstlich 2 Spieler anzulegen oder wie viele
					 * auch immer der "größte" dienst braucht). Da aber sonst die Gesamtzeiten für
					 * ein Team natürlich die Anzahl der gleichzeitig Arbeitenden Personen
					 * berücksichtigt, gibt es andere Stellen, an denen dieser Ausnahmekontext
					 * gilt:
					 * HSGSolver#addZ1GleichVielArbeitsZeit
					 * Statistics#exportStats
					 */
					int nDienste = (HSGApp.TEAMS_ONLY ? 1 : d.getTyp().getPersonen());
					// So viele Zuordnungen wie Personen im Dienst hinzufügen
					for (int i = 0; i < nDienste; i++) {
						res.add(new Zuordnung(p, d, i));
					}
				}
			}
			return res.iterator();
		}).cache();
		if (logger.isDebugEnabled()) {
			logger.debug("Zuordnungen:");
			zuordnungen.foreach(z -> logger.debug(z.getPerson() + "@" + z.getDienst() + "/" + z.getNr()));
		}
		return zuordnungen;
	}

	public static JavaRDD<Zuordnung> entferneDiensteAnNichtspieltagen(final JavaRDD<Game> games, JavaRDD<Zuordnung> zuordnungenAll, JavaPairRDD<HSGDate, Spieltag> spieltage) {
		JavaPairRDD<HSGDate, Iterable<Tuple2<HSGDate, Team>>> gamesByWE
			= games.mapToPair(g -> new Tuple2<>(g.getDate().getWeekend(), new Tuple2<>(g.getDate(), g.getTeam()))).groupByKey();

		logger.info("Zuordnungen vor Wochenendfilter: " + zuordnungenAll.count());
		// Experimentell: Alle Zurordnungen rauswerfen bei denen ein Spieler am jeweils
		// anderen Tag spielt
		JavaRDD<Zuordnung> zuordnungen = zuordnungenAll.groupBy(z -> z.getDienst().getDatum().getWeekend())
			.leftOuterJoin(gamesByWE)
			.flatMap(d -> {
				// Kein Filtern wenn keine
				if (d._1 == null || !d._2._2.isPresent()) {
					return d._2._1.iterator();
				}
				// List<Zuordnung> l = IterableUtil.toList(d._2._1);
				final Set<Team> sa = StreamSupport.stream(d._2._2.get().spliterator(), false)
					.filter(t -> t._1.isSaturday())
					.map(t -> t._2)
					.collect(Collectors.toSet());
				final Set<Team> so = StreamSupport.stream(d._2._2.get().spliterator(), false)
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

	public static List<Dienst> checkAlleNotwendigenZuordnungenSindVorhanden(final JavaPairRDD<HSGDate, Spieltag> spieltage, final JavaRDD<Zuordnung> zuordnungen) {
		List<Dienst> ohneZuordnung = spieltage
			.flatMap(s -> s._2.getDienste().iterator())
			.filter(d -> !d.isElternVerkaufsDienst()) // Nur die nicht schon
														// fixierten
														// elternverkaufsdienste
														// müssen
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

	public static JavaRDD<Zuordnung> processFixed(JavaRDD<Zuordnung> fixed, JavaRDD<Person> personen, JavaRDD<Zuordnung> allzu) {

		fixed = fixed.keyBy(z -> new Tuple2<>(z.getPerson().getName(), z.getPerson().getTeam()))
			.leftOuterJoin(personen.keyBy(p -> new Tuple2<>(p.getName(), p.getTeam())))
			.map(d -> {
				if (d._2._2.isPresent()) {
					return new Zuordnung(d._2._2.get(), d._2._1.getDienst(), d._2._1.getNr());
				} else {
					logger.warn("Person " + d._1 + " aus fixierter Zuordnung " + d._2._1 + " wurde nicht der Personenliste gefunden.");
					return null;
				}
			})
			.filter(d -> d != null)
			.cache();
		fixed.foreach(f -> logger.info("Festgelegt: " + f));

		allzu = allzu.keyBy(z -> new Tuple3<>(z.getDienst(), z.getPerson(), z.getNr()))
			.fullOuterJoin(fixed.keyBy(z -> new Tuple3<>(z.getDienst(), z.getPerson(), z.getNr())))
			.map(d -> {
				if (d._2._1.isPresent()) {
					Zuordnung z = d._2._1.get();
					z.setFixed(d._2._2.isPresent());
					if (d._2._2.isPresent()) {
						logger.warn("Zuordnung fixiert: " + z);
					}
					return z;
				} else {
					logger.warn("Keine Zuordnung für Fixierung: " + d._2._2.get().getDienst().getDatum() + ", " + d._2._2.get());
					return null;
				}
			})
			.filter(d -> d != null)
			.cache();
		return allzu;
	}
}
