/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Person.java
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

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.parquet.Strings;

import dw.tools.hsg.Dienst.Typ;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * TODO Replace with class description.
 *
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
@Data
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Person implements Serializable {

	private static final long serialVersionUID = 3556391185364393215L;
	private static final String AUFSICHT_MARKER = "x";
	public static Logger logger = Logger.getRootLogger();

	private String name;
	private String shortName;
	private Team team;
	private int gearbeitetM;
	private boolean aufsicht;
	private Team trainerVon = null;

	public Person(final String name, final Team team, final int worked, final boolean aufsicht, final Team trainerVon) {
		this(name, createShort(name), team, worked, aufsicht, trainerVon);
	}

	public Person(final String name, final String teamId, final int worked, final boolean aufsicht,
			final String trainerVon) {
		this(name, createShort(name), Team.valueOf(teamId), worked, aufsicht, Team.valueOf(trainerVon));
	}

	public Person(final String name, final String teamId, final int worked) {
		this(name, Team.valueOf(teamId), worked, false, null);
	}

	/*
	 * Jemand ist zulässig für Arbeitsdienste wenn ihr Team in der Liste der
	 * arbeitenden Teams ist oder sie als Aufsicht markiert ist.
	 */
	public boolean mayWork() {
		return (team.mayWork()) || aufsicht;
	}

	/**
	 * Prüft ob eine Person an einem Dienst arbeiten kann.
	 * 
	 * Kriterien:
	 * - Der Dienst ist Aufsicht und die Person ist Aufsicht
	 * - Der Typ ist nicht Aufsicht und das Team darf dort arbeiten.
	 * 
	 * @param d
	 * @return
	 */
	public boolean mayWorkAt(final Dienst d) {
		return team.mayWorkAtTimesOf(d)
				&& (Typ.Wischen != d.getTyp() || HSGApp.WISCHER_DIENSTE.contains(getTeam()))
				&& (Typ.Wischen == d.getTyp() || !HSGApp.WISCHER_DIENSTE.contains(getTeam()))
				&& (Typ.Aufsicht != d.getTyp() || isAufsicht()) // Spieler machen keine Aufsicht
				&& (Typ.Aufsicht == d.getTyp() || !isAufsicht()); // Aufsicht macht keine anderen Dienste
	}

	private static String createShort(final String name) {
		if (name.contains(" ")) {
			String[] parts = name.split(" ");
			return parts[0].substring(0, 1) + parts[1].substring(0, 1);
		}
		return name;
	}

	/**
	 * CSV-Struktur: Name, Team, TrainerVon, Aufsicht, Arbeitsstd
	 * 
	 * @param line
	 * @return
	 */
	public static Person parse(String line) {
		// Google drive seems to export the whole line quoted in ", so we remove that.
		if (line.startsWith("\"")) {
			line = line.substring(1, line.length() - 1);
		}
		String[] elems = line.split(";");
		try {
			boolean aufsicht = !Strings.isNullOrEmpty(elems[3]) && AUFSICHT_MARKER.equalsIgnoreCase(elems[3]);
			Team team = aufsicht ? Team.Aufsicht : (!Strings.isNullOrEmpty(elems[1]) ? Team.valueOf(elems[1]) : null);
			Team trainerVon = !Strings.isNullOrEmpty(elems[2]) ? Team.valueOf(elems[2]) : null;
			if (team == null && !aufsicht) {
				logger.warn("Nicht berechnungsrelevante Person gefunden: " + line);
				return null;
			}
			int worked = (int) Math.round(Double.parseDouble(elems[4].replace(",", ".")) * 60);
			return new Person(elems[0].trim(), team, worked, aufsicht, trainerVon);
		} catch (Exception e) {
			logger.error("Fehler beim Parsen von Person " + line, e);
			throw e;
		}
	}

	@Override
	public String toString() {
		return name + "@" + team + (aufsicht ? "/!" : "");
	}

	public Person teamRepresentant() {
		return team == Team.Aufsicht ? this : new Person(team.name(), team.name(), team, 0, false, null);
	}

}
