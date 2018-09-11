package dw.tools.hsg;
/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Game.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     04.09.2018
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2018, all rights reserved
 * _____________________________________________________________________________
 */

import java.io.Serializable;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.spark_project.guava.base.Strings;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * TODO Replace with class description.
 *
 * @version $Revision$
 * @author wirtzd
 * @since 04.09.2018
 */
@Data
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Game implements Serializable, Comparable<Game> {

	private static final long serialVersionUID = -2732016441985251070L;

	private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");

	private HSGDate date;
	private LocalTime zeit;
	private String halle;
	private String heim;
	private String teamId;
	private String gast;
	private boolean heimspiel;
	private String staffel;

	public static Game parse(final String l) {
		String[] elems = l.split(";");
		Game res = new Game();
		res.staffel = elems[1].trim();
		res.date = new HSGDate(elems[2]);
		res.zeit = LocalTime.from(TIME_FMT.parse(elems[3]));
		res.halle = elems[4].trim();
		res.heim = elems[5].trim();
		res.gast = elems[6].trim();
		res.heimspiel = HSGApp.GA.equals(res.halle);
		String team = res.heimspiel ? res.heim : res.gast;
		String snr = team.length() > 27 ? team.substring(team.length() - 1, team.length()) : null;
		int nr = Strings.isNullOrEmpty(snr) ? 1 : Integer.parseInt(snr);
		res.teamId = res.staffel.startsWith("M") ? "M" + nr
				: res.staffel.startsWith("F") ? "F" + nr
						: res.staffel.contains("-") ? res.staffel.substring(0, res.staffel.lastIndexOf("-")) + nr
								: res.staffel;
		return res;
	}

	@Override
	public int compareTo(Game o) {
		int res = date.compareTo(o.date);
		if (res == 0) {
			return zeit.compareTo(o.zeit);
		}
		return res;
	}

	public String toString() {
		return zeit + " " + staffel + ": " + heim + "-" + gast;
	}

}
