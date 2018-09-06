/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    HSGSolverTest.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     06.09.2018
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2018, all rights reserved
 * _____________________________________________________________________________
 */
package hsgplanner;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import dw.tools.hsg.Dienst;
import dw.tools.hsg.Dienst.Typ;
import dw.tools.hsg.HSGDate;
import dw.tools.hsg.HSGInterval;
import dw.tools.hsg.HSGSolver;
import dw.tools.hsg.Person;
import dw.tools.hsg.Zuordnung;

/**
 * @version $Revision$
 * @author wirtzd
 * @since 06.09.2018
 */
public class HSGSolverTest {

    static JavaSparkContext jsc;

    static {
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

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("HSG Tool").set("spark.ui.enabled", "false")
                 .set("spark.eventLog.enabled", "false")
                 .setMaster("local[*]");
        jsc = new JavaSparkContext(sparkConf);
    }

    HSGDate date = new HSGDate(2018, 9, 6);
    HSGInterval i1100_1300 = new HSGInterval(LocalTime.of(11, 00), LocalTime.of(13, 00));
    HSGInterval i1300_1600 = new HSGInterval(LocalTime.of(13, 00), LocalTime.of(16, 00));
    HSGInterval i1200_1600 = new HSGInterval(LocalTime.of(12, 00), LocalTime.of(16, 00));
    HSGInterval i1500_1900 = new HSGInterval(LocalTime.of(15, 00), LocalTime.of(19, 00));
    Person M1A = new Person("M1A", "M1", 0);
    Person M1B = new Person("M1B", "M1", 0);
    Person M1C = new Person("M1C", "M1", 0);
    Person F1A = new Person("F1A", "F1", 0);
    Person F1B = new Person("F1B", "F1", 0);
    Person F1C = new Person("F1C", "F1", 0);
    Dienst V1100_1300 = new Dienst(date, i1100_1300, Typ.Verkauf);
    Dienst V1300_1600 = new Dienst(date, i1300_1600, Typ.Verkauf);
    Dienst K1200_1600 = new Dienst(date, i1200_1600, Typ.Kasse);

    public static void main(final String[] args) {
        new HSGSolverTest().test3();
    }

    public void test1() {
        List<Person> ps = Arrays.asList(M1A, M1B);
        List<Dienst> ds = Arrays.asList(V1100_1300, V1300_1600);
        List<Zuordnung> all = new ArrayList<>();
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d))));
        HSGSolver.solve(jsc.parallelize(all));
    }

    public void test2() {
        List<Person> ps = Arrays.asList(M1A, M1B, M1C);
        List<Dienst> ds = Arrays.asList(V1100_1300, V1300_1600, K1200_1600);
        List<Zuordnung> all = new ArrayList<>();
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d))));
        HSGSolver.solve(jsc.parallelize(all));
    }

    public void test3() {
        List<Person> ps = Arrays.asList(M1A, M1B, M1C);
        List<Dienst> ds = Arrays.asList(V1100_1300, V1300_1600, V1300_1600, K1200_1600);
        List<Zuordnung> all = new ArrayList<>();
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d))));
        HSGSolver.solve(jsc.parallelize(all));
    }

}
