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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import dw.tools.hsg.Dienst;
import dw.tools.hsg.Dienst.Typ;
import dw.tools.hsg.Game;
import dw.tools.hsg.HSGApp;
import dw.tools.hsg.HSGDate;
import dw.tools.hsg.HSGInterval;
import dw.tools.hsg.HSGSolver;
import dw.tools.hsg.Person;
import dw.tools.hsg.Team;
import dw.tools.hsg.Zuordnung;

/**
 * @author wirtzd
 * @since 06.09.2018
 */
public class HSGSolverTest {

    static JavaSparkContext jsc;

    @BeforeClass
    public static void initStatic() {
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
        sparkConf.setAppName("HSG Tool").set("spark.ui.enabled", "false").set("spark.eventLog.enabled",
                                                                              "false")
                 .setMaster("local[*]");
        jsc = new JavaSparkContext(sparkConf);
    }

    @AfterClass
    public static void finishStatic() {
        jsc.close();
    }

    HSGDate date = new HSGDate(2018, 9, 6);
    HSGInterval i1100_1300 = new HSGInterval(LocalTime.of(11, 00), LocalTime.of(13, 00));
    HSGInterval i1300_1600 = new HSGInterval(LocalTime.of(13, 00), LocalTime.of(16, 00));
    HSGInterval i1200_1600 = new HSGInterval(LocalTime.of(12, 00), LocalTime.of(16, 00));
    HSGInterval i1500_1900 = new HSGInterval(LocalTime.of(15, 00), LocalTime.of(19, 00));
    HSGInterval i1600_2000 = new HSGInterval(LocalTime.of(16, 00), LocalTime.of(20, 00));
    HSGInterval i2000_2200 = new HSGInterval(LocalTime.of(20, 00), LocalTime.of(22, 00));
    Person M1A = new Person("M1A", "M1", 0);
    Person M1B = new Person("M1B", "M1", 0);
    Person M1C = new Person("M1C", "M1", 0);
    Person F1A = new Person("F1A", "F1", 0);
    Person F1B = new Person("F1B", "F1", 0);
    Person F1C = new Person("F1C", "F1", 0);
    Person F2A = new Person("F2A", "F2", 0);
    Person F2B = new Person("F2B", "F2", 0);
    Person F2C = new Person("F2C", "F2", 0);
    Person F3A = new Person("F3A", "F3", 0);
    Person F3B = new Person("F3B", "F3", 0);
    Person F3C = new Person("F3C", "F3", 0);
    Person mA11 = new Person("mA11", "mA1", 0);
    Person mA12 = new Person("mA12", "mA1", 0);
    Person mB11 = new Person("mB11", "mB1", 0);
    Person mB12 = new Person("mB12", "mB1", 0);
    Person A1 = new Person("A1", "M1", 0, true, null);
    Person A2 = new Person("A1", "F1", 0, true, null);
    Person A3 = new Person("A2", "HA", 0, true, null);
    Dienst V1100_1300 = new Dienst(date, i1100_1300, Typ.Verkauf);
    Dienst V1300_1600 = new Dienst(date, i1300_1600, Typ.Verkauf);
    Dienst K1200_1600 = new Dienst(date, i1200_1600, Typ.Kasse);

    Game F21 = new Game(date, LocalTime.of(16, 00), HSGApp.GA, "F2", new Team("F2"), "Foobars", true, "LL");
    Game F11 = new Game(date, LocalTime.of(18, 00), HSGApp.GA, "F1", new Team("F1"), "Foobars", true, "BWOL");
    Game M11 = new Game(date, LocalTime.of(20, 00), HSGApp.GA, "M1", new Team("M1"), "Foobars", true, "BK");
    Game M21 = new Game(date, LocalTime.of(14, 00), HSGApp.GA, "M2", new Team("M2"), "Foobars", true, "BK");
    List<Game> noGames = Collections.emptyList();
    // Game F22 = new Game(date, i1300_1600.getStart(), HSGApp.GA, "F2", new Team("F1"), "Foobars", true, "BK");

    public static void main(final String[] args) throws IOException, ParseException {
        // initStatic();
        // new HSGSolverTest().testAufsicht();
        // finishStatic();
        new HSGSolverTest().testMain();
    }

    @Test
    public void test1() {
        List<Person> ps = Arrays.asList(M1A, M1B);
        List<Dienst> ds = Arrays.asList(V1100_1300, V1300_1600);
        List<Zuordnung> all = new ArrayList<>();
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d))));
        assertEquals(2, HSGSolver.solve(jsc.parallelize(all), jsc.parallelize(noGames)).size());
    }

    @Test
    public void test2() {
        List<Person> ps = Arrays.asList(M1A, M1B, M1C);
        List<Dienst> ds = Arrays.asList(V1100_1300, V1300_1600, K1200_1600);
        List<Zuordnung> all = new ArrayList<>();
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d))));
        assertEquals(3, HSGSolver.solve(jsc.parallelize(all), jsc.parallelize(noGames)).size());
    }

    @Test
    public void test3() {
        List<Person> ps = Arrays.asList(M1A, M1B, M1C, F1A, F1B);
        List<Dienst> ds = Arrays.asList(V1300_1600);
        List<Zuordnung> all = new ArrayList<>();
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d))));
        ps.forEach(p -> ds.forEach(d -> all.add(new Zuordnung(p, d, 2))));
        assertEquals(2, HSGSolver.solve(jsc.parallelize(all), jsc.parallelize(noGames)).size());
    }

    @Test
    public void testAufsicht() {
        List<Person> ps = Arrays.asList(M1A, M1B, M1C, F1A, A1, A2);
        List<Game> gs = Arrays.asList(F21, F11);

        JavaRDD<Game> gsRDD = jsc.parallelize(gs);
        List<Zuordnung> res = HSGApp.compute(gsRDD, jsc.parallelize(ps));
        assertTrue(res.size() > 0);
        List<String> csv = HSGApp.toCSV(gsRDD, jsc.parallelize(res)).collect();
        System.out.println(csv);
    }

    @Test
    public void testAllowedTimes() {
        List<Person> ps = Arrays.asList(M1A, M1B, M1C, F1A, F1B, F3A, F3B, F3C, mA11, mA12, mB11, mB12, A2, A3);
        List<Game> gs = Arrays.asList(F21, F11, M11);

        JavaRDD<Game> gsRDD = jsc.parallelize(gs);
        List<Zuordnung> res = HSGApp.compute(gsRDD, jsc.parallelize(ps));
        assertTrue(res.size() > 0);
        List<String> csv = HSGApp.toCSV(gsRDD, jsc.parallelize(res)).collect();
        System.out.println(csv);
    }

    @Test
    public void testMain() throws IOException, ParseException {
        // HSGApp.main(new String[]
        // {"F:\\Software\\hsgplanner\\src\\test\\resources\\HSG_Leinfelden-Echterdingen_10.csv" ,
        // "F:\\Software\\hsgplanner\\src\\test\\resources\\PersonenM1F1.csv"});
        // HSGApp.main(new String[]
        // {"F:\\Software\\hsgplanner\\src\\test\\resources\\HSG_Leinfelden-Echterdingen_10.csv" ,
        // "F:\\Software\\hsgplanner\\src\\test\\resources\\PersonenM1F1F2.csv"});
        // HSGApp.main(new String[]
        // {"F:\\Software\\hsgplanner\\src\\test\\resources\\HSG_Leinfelden-Echterdingen_10.csv" ,
        // "F:\\Software\\hsgplanner\\src\\test\\resources\\Personen.csv"});
        HSGApp.main(new String[] { "src\\test\\resources\\HSG_Leinfelden-Echterdingen_utf8_30.csv",
                        "src\\test\\resources\\Personen.csv" });
    }
    
    @Test
    public void testDienstLänge() {
    	System.out.println(HSGApp.optimaleDienstlänge(LocalTime.of(14, 0), LocalTime.of(20, 0), Typ.Aufsicht));
    	System.out.println(HSGApp.verteileDienste(new HSGInterval(LocalTime.of(14, 0), LocalTime.of(20, 0)), Typ.Aufsicht));
    }

}
