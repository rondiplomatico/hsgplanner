package dw.tools.hsg;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import dw.tools.hsg.data.Dienst;
import dw.tools.hsg.data.Game;
import dw.tools.hsg.data.HSGDate;
import dw.tools.hsg.data.Person;
import dw.tools.hsg.data.Spieltag;
import dw.tools.hsg.data.Team;
import dw.tools.hsg.data.Zuordnung;
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

	public static final Logger logger = Logger.getLogger(HSGApp.class);

	public static final String GA = "4066";
	public static final String HSGLE = "HSG Leinfelden-Echterdingen";
	public static final String CSV_DELIM = ";";

	public static final DecimalFormat DEC_FORMAT = new DecimalFormat("##.##");
	public static final List<Team> ELTERN_DIENSTE = Arrays.asList(Team.mC1,
		Team.mC2,
		Team.wC1,
		Team.wC2,
		Team.gD1,
		Team.gD2,
		Team.wD1,
		Team.wD2,
		Team.gE1,
		Team.gE2,
		Team.wE1,
		Team.wE2,
		Team.gF1,
		Team.gF2);
	public static final List<Team> WISCHER_DIENSTE = Arrays.asList(Team.mC1, Team.mC2, Team.wC1, Team.wC2);

	/**
	 * Wenn das erste Spiel F1 ist, startet die Gesamtarbeitszeit
	 * F1_EARLY_START_MINUTES früher.
	 * Auch die Kasse wird bei F1 um F1_EARLY_START_MINUTES früher eingesetzt.
	 */
	public static final int F1_EARLY_START_MINUTES = 30;

	/**
	 * Flag that configures the app to assign teams only instead of separate
	 * players.
	 */
	public static final boolean TEAMS_ONLY = true;

	/**
	 * Minimum assumed team size for imports.
	 * Only used when TEAMS_ONLY = true
	 */
	public static final int MIN_TEAM_SIZE = 10;

	@SuppressWarnings("unused")
	public static void main(final String[] args) throws IOException, ParseException {
		Path outbase = args.length > 2 ? new Path(args[2]) : new Path(args[0]).getParent();
		// Prepare logger and spark
		JavaSparkContext jsc = initialize(outbase);
		// Personen einlesen
		JavaRDD<Person> personen = IO.ladePersonen(jsc, new Path(args[1]));
		// Spiele einlesen
		JavaRDD<Game> games = IO.ladeSpiele(jsc, new Path(args[0]));

		/*
		 * Berechnet alle dienste, die sich aus den Spielen ergeben
		 */
		JavaPairRDD<HSGDate, List<Dienst>> dienste = Dienste.berechneDienste(games);

		Map<Team, Tuple3<Integer, Double, Integer>> anzUndVorl = Personen.anzahlUndVorleistungJeTeam(personen);
		if (TEAMS_ONLY) {
			personen = Personen.reduziereAufTeamRepräsentanten(personen);
		}

		/*
		 * Ergänzt die Dienstlisten um zeitinfos für personen, die zeitgleich woanders
		 * trainer sind oder spielen.
		 */
		JavaPairRDD<HSGDate, Spieltag> spieltage = Spieltage.berechneSpieltage(dienste, games, personen);

		final Map<Team, Double> zielArbeitszeitProPersonJeTeam = Spieltage.berechneZielarbeitszeiten(anzUndVorl, spieltage);
		if (!TEAMS_ONLY) {
			personen = Personen.entferneDienstzeiterfüller(personen, zielArbeitszeitProPersonJeTeam);
		}

		JavaRDD<Zuordnung> alleMöglichenZuordunungen = Zuordnungen.erzeugeZuordnungen(personen, spieltage);

		/*
		 * Fixierte Zuordnungen einlesen und rausrechnen (aktuell nur für
		 * Spielerbasierte Berechnung anwendbar)
		 */
		if (!TEAMS_ONLY && args.length > 3) {
			JavaRDD<Zuordnung> zuo = IO.ladeZuordnungen(jsc, new Path(args[3]));
			alleMöglichenZuordunungen = Zuordnungen.processFixed(zuo, personen, alleMöglichenZuordunungen);
		}

		JavaRDD<Zuordnung> fixierteZuordnungen = alleMöglichenZuordunungen.filter(Zuordnung::isFixed);
		JavaRDD<Zuordnung> elternDienste = fixierteZuordnungen.filter(z -> z.getDienst().isElternVerkaufsDienst());
		logger.info("Anzahl Zuordnungen: " + alleMöglichenZuordunungen.count()
			+ ", darin feste Zuordnungen: " + fixierteZuordnungen.count()
			+ ", davon feste Zuordnungen durch Elterndienste: " + elternDienste.count());

		JavaRDD<Zuordnung> freieZuordnungen
			= Zuordnungen.entferneDiensteAnNichtspieltagen(games, alleMöglichenZuordunungen.filter(z -> !z.isFixed()), spieltage);

		if (!Zuordnungen.checkAlleNotwendigenZuordnungenSindVorhanden(spieltage, freieZuordnungen).isEmpty()) {
			jsc.close();
			throw new RuntimeException("Zu wenige Zuordnungen generiert!");
		}

//		freieZuordnungen = freieZuordnungen.filter(z -> Team.mC1.equals(z.getPerson().getTeam()) || Team.wC1.equals(z.getPerson().getTeam()));

		// Gibt den Diensterahmen ohne Zuordnungen aus
		JavaRDD<String> content = IO.toCSV(games, freieZuordnungen.union(fixierteZuordnungen));
		Path out = new Path(outbase, "dienste_rahmen.csv");
//		saveAsFile(content, out);

//		final Map<Typ, Integer> zeitenNachTyp = new HashMap<>(
//				spieltage.flatMap(s -> s._2.dienste.iterator()).distinct().keyBy(d -> d.getTyp()).aggregateByKey(0,
//						(ex, n) -> ex + n.zeit.dauerInMin() * n.getTyp().getPersonen(),
//						(a, b) -> a + b).collectAsMap());
//		zeitenNachTyp.forEach(
//				(t, z) -> logger.info("Gesamt zu leistende Zeit für " + t + ": " + z + " (" + (z / 60.0) + "h)"));

		JavaRDD<Zuordnung> zu = HSGSolver.solve(freieZuordnungen, games, zielArbeitszeitProPersonJeTeam)
			.union(fixierteZuordnungen)
			.cache();

		content = IO.toCSV(games, zu);
		out = new Path(outbase, "dienste.csv");
		IO.saveAsFile(content, out);

		Statistics.exportStats(jsc, outbase, zu, personen, zielArbeitszeitProPersonJeTeam);

		jsc.close();
	}

	private static JavaSparkContext initialize(Path outbase) {
		// Log output to import folder
		FileAppender fa = new FileAppender();
		fa.setFile(new Path(outbase, "convert.log").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		Logger.getRootLogger().addAppender(fa);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("HSG Tool")
			.set("spark.ui.enabled", "true")
			.set("spark.eventLog.enabled", "false")
			.setMaster("local[*]");
		return new JavaSparkContext(sparkConf);
	}

}
