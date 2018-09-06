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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;

import net.sf.javailp.Constraint;
import net.sf.javailp.Linear;
import net.sf.javailp.Operator;
import net.sf.javailp.OptType;
import net.sf.javailp.Problem;
import net.sf.javailp.Result;
import net.sf.javailp.Solver;
import net.sf.javailp.SolverFactory;
import net.sf.javailp.SolverFactoryLpSolve;
import net.sf.javailp.Term;
import scala.Tuple2;

/**
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
public class HSGSolver {

	private static final String AVERAGE_WORK_TIME = "AverageWorkTime";
	private static int substCnt = 1;

	public static List<Zuordnung> solve(final JavaRDD<Zuordnung> all) {
		final Problem problem = new Problem();

		List<Zuordnung> allList = all.collect();

		/*
		 * ********************************************************* Zielfunktion
		 * *********************************************************
		 */
		// Init
		System.out.println("Erstelle Zielfunktion");
		Linear target = new Linear();
		for (Zuordnung z : allList) {
			problem.setVarType(z.varName(), Boolean.class);
		}
		addZ1GleichVielArbeitsZeit(all, problem, target);

		// addZ2GleichesTeamFavorisiertZusammen()
		// addZ3DienstKurzVorEigenemSpiel()

		problem.setObjective(target, OptType.MIN);

		/*
		 * ********************************************************* Nebenbedingungen
		 * *********************************************************
		 */
		/*
		 * Nur eine Person pro Dienst
		 */
		System.out.println("Erstelle NB: Nur eine Person pro Dienst");
		addN1EinePersonProDienst(all, problem);

		/*
		 * Pro Person nur ein Dienst gleichzeitig. Da die Dienste beliebig überschneiden
		 * können, muss man vorher disjunkt aufteilen.
		 */
		System.out.println("Erstelle NB: Keine parallelen Dienste pro Person");
		addN2KeineParallelenDienste(all, problem);

		addN3NurGleicheTeamsInGleichenDiensten(all, problem);

		/*
		 * ********************************************************* Lösen
		 * *********************************************************
		 */

		System.out.println("Problem:");
		System.out.println(problem);
		
		System.out.println("Löse");
		// SolverFactory factory = new SolverFactoryGurobi();
		// SolverFactory factory = new SolverFactorySAT4J();
		// SolverFactory factory = new SolverFactoryMiniSat();
		SolverFactory factory = new SolverFactoryLpSolve();
		factory.setParameter(Solver.VERBOSE, 4);
		factory.setParameter(Solver.TIMEOUT, 10000); // set timeout to 100 seconds
		Solver solver = factory.get(); // you should use this solver only once for one problem
		
		Result result = solver.solve(problem);
		System.out.println(result);

		System.out.println("Ergebnis:");
		List<Zuordnung> selected = allList.stream().filter(z -> result.getBoolean(z.varName()))
				.collect(Collectors.toList());
		selected.forEach(System.out::println);

		System.out.println("Durchschnittliche Arbeitszeit:" + result.getPrimalValue(AVERAGE_WORK_TIME));
		return selected;
	}

	private static void addN3NurGleicheTeamsInGleichenDiensten(final JavaRDD<Zuordnung> all, final Problem problem) {
		all.mapToPair(z -> new Tuple2<>(new Tuple2<>(z.getDienst(), z.getPerson().getTeamId()), z.varName()))
				.aggregateByKey(new ArrayList<String>(), (ex, n) -> {
					ex.add(n);
					return ex;
				}, (a, b) -> {
					a.addAll(b);
					return a;
				}).values().filter(d -> d.size() > 1).collect().forEach(l -> {
					for (String lead : l) {
						Linear li = new Linear();
						li.add(-1, lead);
						for (String added : l) {
							if (added != lead) {
								li.add(1, added);
							}
						}
						problem.add(li, Operator.GE, 0);
					}
				});
	}

	private static void addN2KeineParallelenDienste(final JavaRDD<Zuordnung> all, final Problem problem) {
		all.mapToPair(t -> new Tuple2<>(new Tuple2<>(t.getPerson(), t.getDienst().getDatum()),
				new Tuple2<>(t.varName(), t.getDienst().getZeit()))).groupByKey().mapValues(v -> {
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
						s.stream().flatMap(i -> idsPerTime.get(i).stream()).forEach(res1::add);
						res.add(res1);
					}
					return res;
				}).collect().forEach(t -> {
					for (List<String> timedata : t._2) {
						if (timedata.size() > 1) {
							Linear l = new Linear();
							timedata.forEach(id -> l.add(1, id));
							problem.add(new Constraint(l, Operator.LE, 1));
						}
					}
				});
	}

	private static void addN1EinePersonProDienst(final JavaRDD<Zuordnung> all, final Problem problem) {
		all.mapToPair(t -> new Tuple2<>(new Tuple2<>(t.getDienst(), t.getNr()), t.varName())).groupByKey().mapValues(IterableUtil::toList)
				.collectAsMap().forEach((d, zall) -> {
					Linear l = new Linear();
					zall.forEach(z -> l.add(1, z));
					problem.add(new Constraint(l, Operator.EQ, 1));
				});
	}

	private static void addZ1GleichVielArbeitsZeit(final JavaRDD<Zuordnung> all, final Problem problem,
			final Linear target) {
		/*
		 * Alle gleich viel Arbeitszeit. Die gesamtarbeitszeit soll pro person nicht von
		 * der (aktuellen) durchschnittlichen arbeitszeit abweichen.
		 */
		final List<String> allTotalTimesPerPerson = new ArrayList<>();
		all.mapToPair(z -> new Tuple2<>(z.getPerson(), new Tuple2<>(z.varName(), z.getDienst().getZeit().dauerInMin())))
				.groupByKey().mapValues(IterableUtil::toList).collect().forEach(d -> {
					Linear linear = new Linear();
					d._2.forEach(t -> linear.add(t._2, t._1));
					String sumVarName = "TotalZeit" + d._1.getName();
					linear.add(-1, sumVarName);
					problem.setVarType(sumVarName, Integer.class);
					problem.setVarLowerBound(sumVarName, 0);
					problem.add(new Constraint(linear, Operator.EQ, -d._1.getGearbeitetM()));
					allTotalTimesPerPerson.add(sumVarName);
				});
		Linear avg = new Linear();
		double numPInv = 1 / (double) allTotalTimesPerPerson.size();
		allTotalTimesPerPerson.forEach(s -> avg.add(numPInv, s));
		createSubstitute(problem, avg, AVERAGE_WORK_TIME);

		allTotalTimesPerPerson.forEach(s -> {
			Linear l = new Linear();
			String plus = s + "+";
			String minus = s + "-";
			l.add(new Term(s, 1), new Term(AVERAGE_WORK_TIME, -1), new Term(plus, 1), new Term(minus, -1));
			problem.add(new Constraint(l, Operator.EQ, 0));
			problem.setVarType(plus, Integer.class);
			problem.setVarType(minus, Integer.class);
			problem.setVarLowerBound(plus, 0);
			problem.setVarLowerBound(minus, 0);
			target.add(new Term(plus, 1), new Term(minus, 1));
		});
	}

	private static String createSubstitute(final Problem p, final Linear linear) {
		String name = "sub" + substCnt++;
		createSubstitute(p, linear, name);
		return name;
	}

	private static void createSubstitute(final Problem p, final Linear linear, final String name) {
		linear.add(-1, name);
		p.setVarType(name, Integer.class);
		p.add(new Constraint(linear, Operator.EQ, 0));
	}
}
