/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Team.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     11.09.2018
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2018, all rights reserved
 * _____________________________________________________________________________
 */
package dw.tools.hsg;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @version $Revision$
 * @author wirtzd
 * @since 11.09.2018
 */
@RequiredArgsConstructor
@Getter
public enum Team {

	M1(1.0, HSGInterval.ALL_DAY, true), M2(1.0, HSGInterval.ALL_DAY), M3(1.0, HSGInterval.ALL_DAY),
	M4(1.0, HSGInterval.ALL_DAY), F1(1.0, HSGInterval.ALL_DAY, true), F2(1.0, HSGInterval.ALL_DAY, true),
	F3(1.0, HSGInterval.ALL_DAY), mA1(0.75, HSGInterval.BIS_17), mA2(0.75, HSGInterval.BIS_17),
	wA1(0.75, HSGInterval.BIS_17), wA2(0.75, HSGInterval.BIS_17), mB1(0.75, HSGInterval.BIS_17),
	mB2(0.75, HSGInterval.BIS_17), wB1(0.75, HSGInterval.BIS_17), wB2(0.75, HSGInterval.BIS_17),
	mC(0.75, HSGInterval.BIS_15), wC(0.75, HSGInterval.BIS_15), mD(0.75, HSGInterval.BIS_15),
	wD(0.75, HSGInterval.BIS_15), Aufsicht(1, HSGInterval.ALL_DAY), E(0.0, HSGInterval.EMPTY),
	F(0.0, HSGInterval.EMPTY), Minis(0.0, HSGInterval.EMPTY), None(1, HSGInterval.ALL_DAY);

	public static final Map<Team, HSGInterval> WORKING_TEAMS = teamTimes();

	private final double leistungsFaktor;
	private final HSGInterval workTime;
	private final boolean mitKasse;

	private Team(double leistungsfaktor, HSGInterval workTime) {
		this(leistungsfaktor, workTime, false);
	}

	private static Map<Team, HSGInterval> teamTimes() {
		Map<Team, HSGInterval> res = new HashMap<>();
		Arrays.asList(M1, M2, M3, M4, F1, F2, F3).forEach(s -> res.put(s, HSGInterval.ALL_DAY));
		Arrays.asList(mA1, mA2, wA1, wA2).forEach(
				s -> res.put(s, new HSGInterval(LocalTime.MIN, LocalTime.of(17, 00))));
		Arrays.asList(mB1, mB2, wB1, wB2).forEach(
				s -> res.put(s, new HSGInterval(LocalTime.MIN, LocalTime.of(15, 00))));
		Arrays.asList(mB1, mB2, wB1, wB2).forEach(
				s -> res.put(s, new HSGInterval(LocalTime.MIN, LocalTime.of(15, 00))));
		return res;
	}

	public boolean mayWorkAt(final Dienst d) {
		return workTime.contains(d.getZeit());
	}

	public boolean mayWork() {
		return !workTime.isEmpty();
	}

	public boolean isJugend() {
		return !name().startsWith("M") && !name().startsWith("F");
	}

	public boolean isAktive() {
		return !isJugend() && !Aufsicht.equals(this);
	}

}
