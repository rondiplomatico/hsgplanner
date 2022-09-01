/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Solver.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     05.09.2018
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2018, all rights reserved
 * _____________________________________________________________________________
 */
package dw.tools.hsg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import dw.tools.hsg.Dienst.Typ;
import net.sf.javailp.Constraint;
import net.sf.javailp.Linear;
import net.sf.javailp.NativeLoaderCplex;
import net.sf.javailp.Operator;
import net.sf.javailp.OptType;
import net.sf.javailp.Problem;
import net.sf.javailp.Result;
import net.sf.javailp.SOS;
import net.sf.javailp.SOS.SOSType;
import net.sf.javailp.Solver;
import net.sf.javailp.SolverFactory;
import net.sf.javailp.SolverFactoryCPLEX;
import net.sf.javailp.SolverParameter;
import net.sf.javailp.Term;
import net.sf.javailp.VarType;
import scala.Tuple2;
import scala.Tuple3;

/**
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
public class HSGSolver {

	private static int substCnt = 1;
	public static final int CLEVERE_DIENSTE_MAX_ABSTAND_MINUTEN = 60;
	public static final int DEFAULT_ZUORDNUNG_WEIGHT = CLEVERE_DIENSTE_MAX_ABSTAND_MINUTEN + 30;
	public static final int ÜBERZEIT_STRAFE = 5;
	public static final int UNTERZEIT_STRAFE = 1;
	public static final int AUFSICHT_SOLVE_MINUTES = 3;
	public static final int KV_SOLVE_MINUTES = 10;

	public static Logger logger = Logger.getLogger(HSGSolver.class);

	public static List<Zuordnung> solve(final JavaRDD<Zuordnung> all, final JavaRDD<Game> games,
			Map<Team, Double> avgZeitProTeam) {

		List<Zuordnung> res = new ArrayList<>();

		// Aufsicht
		Problem aufsicht = assembleAufsicht(all.filter(z -> z	.getDienst()
																.getTyp()
																.equals(Typ.Aufsicht)), games, avgZeitProTeam);
		res.addAll(solveProblem(aufsicht, all, 60 * AUFSICHT_SOLVE_MINUTES));

		// Kasse, Verkauf
		Problem kasseVerkaufWischer = assembleVerkaufKasseWischer(all.filter(z -> !z	.getDienst()
																		.getTyp()
																		.equals(Typ.Aufsicht)), games, avgZeitProTeam);
		res.addAll(solveProblem(kasseVerkaufWischer, all, 60 * KV_SOLVE_MINUTES));

		return res;
	}

	private static Problem assembleVerkaufKasseWischer(final JavaRDD<Zuordnung> all, final JavaRDD<Game> games,
			Map<Team, Double> avgZeitProTeam) {
		final Problem problem = new Problem();

		List<Zuordnung> allList = all.collect();

		if (allList.stream().filter(z -> z.getPerson().isAufsicht()).count() > 0) {
			allList.stream().filter(z -> z.getPerson().isAufsicht()).forEach(System.err::println);
			throw new RuntimeException("Aufsichtspersonal darf nicht für Kasse/Verkauf verwendet werden.");
		}

		/*
		 * ********************************************************* Zielfunktion
		 * *********************************************************
		 */
		// Init
		logger.info("Erstelle Zielfunktion");

		Linear target = new Linear();
		for (Zuordnung z : allList) {
			problem.setVarType(z.varName(), Boolean.class);
		}
		logger.info("Z1: Gleiche mittlere Arbeitszeiten");

		// Aufsicht und Verkauf/Kasse haben eigene mittlere Arbeitszeiten
		addZ1GleichVielArbeitsZeit(all, problem, target, false, avgZeitProTeam);

		logger.info("Z2: Geschickte Dienste um eigene Spiele");
		addZ2DienstNahBeiEigenemSpiel(all, games, problem, target);

		problem.setObjective(target, OptType.MIN);

		/*
		 * ********************************************************* Nebenbedingungen
		 * *********************************************************
		 */
		/*
		 * Nur eine Person pro Dienst
		 */
		logger.info("Erstelle N1: Nur eine Person pro Dienst");
		addN1EinePersonProDienst(all, problem);

		/*
		 * Pro Person nur ein Dienst gleichzeitig. Da die Dienste beliebig überschneiden
		 * können, muss man vorher disjunkt aufteilen.
		 */
		logger.info("Erstelle N2: Keine parallelen Dienste pro Person");
		if (!HSGApp.TEAMS_ONLY) {
			addN2KeineParallelenDienste(all, problem);

			logger.info("Erstelle N3: Nur gleiche Teams in gleichen Diensten");
			addN3NurGleicheTeamsInGleichenDiensten(all, problem);
		}

		logger.info("Erstelle N4: Schon fixierte Zuordnungen");
		addN4FixierteZuordnungen(allList, problem);

		return problem;
	}

	public static Problem assembleAufsicht(final JavaRDD<Zuordnung> all, final JavaRDD<Game> games,
			Map<Team, Double> avgZeitProTeam) {
		final Problem problem = new Problem();

		List<Zuordnung> allList = all.collect();
		if (allList.stream().filter(z -> !z.getPerson().isAufsicht()).count() > 0) {
			allList.stream().filter(z -> !z.getPerson().isAufsicht()).forEach(System.err::println);
			throw new RuntimeException("Spieler dürfen nicht für Aufsicht verwendet werden.");
		}

		/*
		 * ********************************************************* Zielfunktion
		 * *********************************************************
		 */
		// Init
		logger.info("Erstelle Zielfunktion");
		Linear target = new Linear();
		for (Zuordnung z : allList) {
			problem.setVarType(z.varName(), Boolean.class);
		}
		logger.info("Z1: Gleiche mittlere Arbeitszeiten");
		// Aufsicht und Verkauf/Kasse haben eigene mittlere Arbeitszeiten
		addZ1GleichVielArbeitsZeit(all, problem, target, true, avgZeitProTeam);

		logger.info("Z2: Geschickte Dienste um eigene Spiele");
		addZ2DienstNahBeiEigenemSpiel(all, games, problem, target);

		problem.setObjective(target, OptType.MIN);

		/*
		 * ********************************************************* Nebenbedingungen
		 * *********************************************************
		 */
		/*
		 * Nur eine Person pro Dienst
		 */
		logger.info("Erstelle N1: Nur eine Person pro Dienst");
		addN1EinePersonProDienst(all, problem);

		logger.info("Erstelle N4: Schon fixierte Zuordnungen");
		addN4FixierteZuordnungen(allList, problem);

		return problem;
	}

	public static List<Zuordnung> solveProblem(Problem problem, final JavaRDD<Zuordnung> all, int seconds) {
		logger.info("Löse");
		// SolverFactory factory = new SolverFactoryGurobi();
		// SolverFactory factory = new SolverFactorySAT4J();
		// SolverFactory factory = new SolverFactoryMiniSat();

		try {
			NativeLoaderCplex.load();
		} catch (IOException e) {
			throw new RuntimeException("Failed loading CPLEX libraries", e);
		}
		SolverFactory factory = new SolverFactoryCPLEX();

		/*
		 * LP_OPT Verbosity Levels: NEUTRAL (0) Only some specific debug messages in de
		 * debug print routines are reported. CRITICAL (1) Only critical messages are
		 * reported. Hard errors like instability, out of memory, ... SEVERE (2) Only
		 * severe messages are reported. Errors. IMPORTANT (3) Only important messages
		 * are reported. Warnings and Errors. NORMAL (4) Normal messages are reported.
		 * This is the default. DETAILED (5) Detailed messages are reported. Like model
		 * size, continuing B&B improvements, ... FULL (6) All messages are reported.
		 * Useful for debugging purposes and small models.
		 */
		// SolverFactory factory = new SolverFactoryLpSolve();
//        factory.setParameter(Solver.TIMEOUT, 60*60*3); // set timeout to 100 seconds [60 * 60 * 14]
		factory.setParameter(Solver.TIMEOUT, seconds); // set timeout to 100 seconds [60 * 60 * 14]
		Solver solver = factory.get(); // you should use this solver only once for one problem

		solver.setParameter(SolverParameter.RAND_SEED, 1);
		solver.setParameter(SolverParameter.NUMBER_OF_THREADS, 8);
		solver.setParameter(SolverParameter.WORK_DIRECTORY, ".");
		solver.setParameter(SolverParameter.WORKING_MEMORY, 1024 * 10);
		solver.setParameter(SolverParameter.NODE_STORAGE_FILE_SWITCH, 3);
		solver.setParameter(SolverParameter.MEMORY_EMPHASIS, true);
		solver.setParameter(SolverParameter.ADVANCED_START_SWITCH, 0);
		solver.setParameter(SolverParameter.VERBOSE, 5);

		List<Zuordnung> selected = Collections.emptyList();
		Result result = solver.solve(problem);
		if (result != null) {
			logger.info(result);

			logger.info("Ergebnis: ");
			selected = all.collect().stream().filter(z -> result.getBoolean(z.varName())).collect(Collectors.toList());

//			selected.forEach(z -> logger.info(z));
			// logger.info("Durchschnittliche Arbeitszeit:" +
			// result.getPrimalValue(AVERAGE_WORK_TIME));
			// logger.info("Durchschnittliche Arbeitszeit Aufsicht:" +
			// result.getPrimalValue(AVERAGE_WORK_TIME_AUFSICHT));
		} else {
			logger.info("Solve fehlgeschlagen.");
			problem.print();
		}
		return selected;
	}

	private static void addN4FixierteZuordnungen(List<Zuordnung> allList, Problem problem) {
		allList.stream().filter(z -> z.isFixed()).forEach(z -> {
			Linear l = new Linear();
			l.add(1, z.varName());
			problem.add("Fix" + z.varName(), l, Operator.EQ, 1);
		});
	}

	private static void addZ2DienstNahBeiEigenemSpiel(final JavaRDD<Zuordnung> all, final JavaRDD<Game> games,
			final Problem problem, final Linear target) {
		JavaRDD<Tuple3<Person, Integer, String>> tmp =
				all	.keyBy(z -> z.getDienst().getDatum())
					.groupByKey()
					.mapValues(IterableUtil::toList)
					.join(games	.keyBy(g -> g.getDate())
								.groupByKey()
								.mapValues(IterableUtil::toList))
					.flatMap(d -> {
						List<Tuple3<Person, Integer, String>> präferierteDienste =
								new ArrayList<>();
						Set<Zuordnung> processed = new HashSet<>();
						for (Game g : d._2._2) {
							for (Zuordnung z : d._2._1) {
								if (g.isHeimspiel() && g.getTeam()
														.equals(z	.getPerson()
																	.getTeam())) {
									int weight = z	.getDienst()
													.getZeit()
													.minuteDistance(g.getDienstSperrenZeitraum());
									if (weight <= CLEVERE_DIENSTE_MAX_ABSTAND_MINUTEN) {
										// int weight =
										// CLEVERE_DIENSTE_MAX_ABSTAND_MINUTEN -
										// diff;
										// weight = z.getPerson().isAufsicht() ?
										// weight * ARBEITSDIENST_FAKTOR :
										// weight;
										logger.debug(z.getPerson()
												+ " kann geschickt vor/nach Spiel "
												+ g
												+ " den Dienst " + z.getDienst()
												+ " machen. Faktor:" + weight);
										präferierteDienste.add(new Tuple3<>(z
																				.getPerson(),
												weight, z.varName()));
										processed.add(z);
									}
								}
							}
						}
						for (Zuordnung z : d._2._1) {
							if (!processed.contains(z)) {
								präferierteDienste.add(new Tuple3<>(z
																		.getPerson(),
										DEFAULT_ZUORDNUNG_WEIGHT, z.varName()));
							}
						}
						return präferierteDienste.iterator();
					});
		tmp.collect().forEach(t -> {
			target.add(t._2(), t._3());
		});
	}

	private static void addN3NurGleicheTeamsInGleichenDiensten(final JavaRDD<Zuordnung> all, final Problem problem) {
		all	.mapToPair(z -> new Tuple2<>(new Tuple2<>(z.getDienst(), z.getPerson().getTeam()), z))
			.aggregateByKey(new ArrayList<Zuordnung>(), (ex, n) -> {
				ex.add(n);
				return ex;
			}, (a, b) -> {
				a.addAll(b);
				return a;
			})
			.values()
			.filter(d -> d.size() > 1)
			.collect()
			.forEach(l -> {
				for (Zuordnung lead : l) {
					Linear li = new Linear();
					li.add(-1, lead.varName());
					for (Zuordnung added : l) {
						if (!added.getPerson().equals(lead.getPerson()) && added.getNr() != lead.getNr()
								&& !added.varName().equals(lead.varName())) {
							li.add(1, added.varName());
						}
					}
					if (li.size() > 1) {
						problem.add(new Constraint("N3:" + lead.getPerson().getName() + "@"
								+ lead.getDienst().getZeit() + "/" + lead.getNr(), li, Operator.GE, 0));
					}
				}
			});
	}

	private static void addN2KeineParallelenDienste(final JavaRDD<Zuordnung> all, final Problem problem) {
		all	.mapToPair(t -> new Tuple2<>(new Tuple2<>(t.getPerson(), t.getDienst().getDatum()),
				new Tuple2<>(t.varName(), t.getDienst().getZeit())))
			.groupByKey()
			.mapValues(v -> {
				Map<HSGInterval, List<String>> idsPerTime = new HashMap<>();
				for (Tuple2<String, HSGInterval> s : v) {
					if (!idsPerTime.containsKey(s._2)) {
						idsPerTime.put(s._2, new ArrayList<>());
					}
					idsPerTime.get(s._2).add(s._1);
				}
				Map<HSGInterval, List<HSGInterval>> disj = IntervalDisjoiner.disjoin(idsPerTime.keySet());
				List<List<String>> res = new ArrayList<>();
				for (List<HSGInterval> s : disj.values()) {
					List<String> res1 = new ArrayList<>();
					s	.stream()
						.flatMap(i -> idsPerTime.get(i).stream())
						.forEach(res1::add);
					res.add(res1);
				}
				return res;
			})
			.collect()
			.forEach(t -> {
				int num = 0;
				for (List<String> timedata : t._2) {
					if (timedata.size() > 1) {
						Linear l = new Linear();
						timedata.forEach(id -> l.add(1, id));
						problem.add(new Constraint("N2:" + t._1._1.getName() + "@" + t._1._2 + "#" + num++, l,
								Operator.LE, 1));
					}
				}
			});
	}

	private static void addN1EinePersonProDienst(final JavaRDD<Zuordnung> all, final Problem problem) {
		all	.mapToPair(t -> new Tuple2<>(new Tuple2<>(t.getDienst(), t.getNr()), t.varName()))
			.groupByKey()
			.mapValues(IterableUtil::toList)
			.collectAsMap()
			.forEach((d, zall) -> {
				Linear l = new Linear();
				zall.forEach(z -> l.add(1, z));
				problem.add(new SOS("N1:" + d._1 + "/" + d._2, l, SOSType.SOS1));
				// problem.add(new Constraint("N1:" + d._1 + "/" + d._2, l, Operator.EQ, 1));
			});
	}

	private static void addZ1GleichVielArbeitsZeit(final JavaRDD<Zuordnung> all, final Problem problem,
			final Linear target, final boolean isAufsicht, final Map<Team, Double> avgZeitProTeam) {
		/*
		 * Alle gleich viel Arbeitszeit. Die gesamtarbeitszeit soll pro person nicht von
		 * der (aktuellen) durchschnittlichen arbeitszeit abweichen.
		 */
		all	.filter(z -> isAufsicht ? Typ.Aufsicht == z.getDienst().getTyp()
				: Typ.Aufsicht != z.getDienst().getTyp())
			.mapToPair(z -> new Tuple2<>(z.getPerson(),
					new Tuple2<>(z.varName(), z.getDienst().getZeit().dauerInMin())))
			.groupByKey()
			.mapValues(IterableUtil::toList)
			.collect()
			.forEach(d -> {
				Linear linear = new Linear();
				d._2.forEach(t -> linear.add(t._2, t._1));

				String sumVarName = "TotalZeit" + d._1.getName() + "@" + d._1.getTeam().name();
				linear.add(-1, sumVarName);
				problem.setVarType(sumVarName, VarType.INT);
				problem.setVarLowerBound(sumVarName, 0);
				Constraint c = new Constraint(sumVarName, linear, Operator.EQ, 0);
				logger.debug("Adding total time variable for " + d._1.getName() + ": " + c);
				problem.add(c);

				Linear l = new Linear();
				String plus = sumVarName + "+";
				String minus = sumVarName + "-";
				l.add(new Term(sumVarName, 1), new Term(plus, -1), new Term(minus, 1));
				c = new Constraint(sumVarName + "Slack", l, Operator.EQ,
						avgZeitProTeam.getOrDefault(d._1.getTeam(), 0.0) - d._1.getGearbeitetM());
				logger.debug("Adding slacked constraint " + c);
				problem.add(c);
				problem.setVarType(plus, VarType.REAL);
				problem.setVarType(minus, VarType.REAL);
				problem.setVarLowerBound(plus, 0);
				problem.setVarLowerBound(minus, 0);
				target.add(new Term(plus, ÜBERZEIT_STRAFE), new Term(minus, UNTERZEIT_STRAFE));
				logger.debug(sumVarName + ": " + d._1.getGearbeitetM());
			});

//		Linear avg = new Linear();
//		double numPInv = 1 / (double) allTotalTimesPerPerson.size();
//		allTotalTimesPerPerson.forEach(s -> avg.add(numPInv, s._1));
//		createSubstitute(problem, avg, VarType.REAL, isAufsicht ? AVERAGE_WORK_TIME_AUFSICHT : AVERAGE_WORK_TIME);
//
//		allTotalTimesPerPerson.forEach(t -> {
//			String varName = t._1;
//
//		});
	}

	private static String createSubstitute(final Problem p, final Linear linear, final VarType type) {
		String name = "sub" + substCnt++;
		createSubstitute(p, linear, type, name);
		return name;
	}

	private static void createSubstitute(final Problem p, final Linear linear, final VarType type, final String name) {
		linear.add(-1, name);
		p.setVarType(name, type);
		p.add(new Constraint(name, linear, Operator.EQ, 0));
		logger.debug("Erstelle Substitution " + name + ": " + linear);
	}
}
