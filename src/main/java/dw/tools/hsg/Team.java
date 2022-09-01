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

	M1(1.0, HSGInterval.ALL_DAY, true),
	M2(1.0, HSGInterval.ALL_DAY),
	M3(1.0, HSGInterval.ALL_DAY),
	M4(1.0, HSGInterval.ALL_DAY),
	F1(1.0, HSGInterval.ALL_DAY, true, true),
	F2(1.0, HSGInterval.ALL_DAY, true),
	F3(1.0, HSGInterval.ALL_DAY),
	mA1(0.85, HSGInterval.BIS_20), mA2(0.85, HSGInterval.BIS_20),
	wA1(0.85, HSGInterval.BIS_20), wA2(0.85, HSGInterval.BIS_20),
	mB1(0.75, HSGInterval.BIS_18), mB2(0.75, HSGInterval.BIS_18),
	wB1(0.75, HSGInterval.BIS_18), wB2(0.75, HSGInterval.BIS_18),
	mC1(0.75, HSGInterval.BIS_20), wC1(0.75, HSGInterval.BIS_20), // Bis 20:00 Uhr wegen Wischerdiensten.
	mC2(0.75, HSGInterval.BIS_20), wC2(0.75, HSGInterval.BIS_20),
	gD1(0.0, HSGInterval.EMPTY), wD1(0.0, HSGInterval.EMPTY),
	gD2(0.0, HSGInterval.EMPTY), wD2(0.0, HSGInterval.EMPTY),
	gE1(0.0, HSGInterval.EMPTY), gE2(0.0, HSGInterval.EMPTY),
	wE1(0.0, HSGInterval.EMPTY), wE2(0.0, HSGInterval.EMPTY),
	gF1(0.0, HSGInterval.EMPTY), gF2(0.0, HSGInterval.EMPTY), gF3(0.0, HSGInterval.EMPTY),
	Aufsicht(1, HSGInterval.ALL_DAY),
	Minis(0.0, HSGInterval.EMPTY),
	None(1, HSGInterval.ALL_DAY);

	private final double leistungsFaktor;
	private final HSGInterval workTime;
	private final boolean mitKasse;
	private final boolean mitWischer;

	private Team(double leistungsfaktor, HSGInterval workTime, boolean mitKasse) {
		this(leistungsfaktor, workTime, mitKasse, false);
	}

	private Team(double leistungsfaktor, HSGInterval workTime) {
		this(leistungsfaktor, workTime, false);
	}

	public boolean mayWorkAtTimesOf(final Dienst d) {
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

	public Person getEltern() {
		return new Person("Eltern " + name(), this, 0, false, null);
	}

}
