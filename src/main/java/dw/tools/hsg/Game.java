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
import java.time.temporal.ChronoUnit;

import org.spark_project.guava.base.Strings;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * TODO Replace with class description.
 *
 * @version $Revision$
 * @author wirtzd
 * @since 04.09.2018
 */
@Data
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Game implements Serializable, Comparable<Game> {

	private static final long SPERRSTD_NACHLAUF_AUSWÄRTSSPIEL = 3L;
	private static final long SPERRSTD_NACHLAUF_HEIMSPIEL = 2L;
	private static final long SPERRSTD_VORLAUF_AUSWÄRTSSPIEL = 2L;
	private static final long SPERRSTD_VORLAUF_HEIMSPIEL = 1L;

	private static final long serialVersionUID = -2732016441985251070L;

	private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");

	private HSGDate date;
	private LocalTime zeit;
	private String halle;
	private String heim;
	@Getter
	private Team team;
	private String gast;
	private boolean heimspiel;
	private String staffel;

	public static Game parse(final String l) {
		/*
		 * Skip first header line, which begins with "Nummer"
		 */
		if (l.startsWith("\"Nummer\"")) {
			return null;
		}
		try {
			String[] elems = l.replace("\"", "").split(";");
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
			/*
			 * MXXX -> M[1-N] FXXX -> F[1-N] XJYZ -> XYZ[1-N]
			 */
			res.team = Team.valueOf(res.staffel.startsWith("M") ? "M" + nr
					: res.staffel.startsWith("F") ? "F" + nr
							: res.staffel.contains("-")
									? res.staffel.substring(0, res.staffel.lastIndexOf("-")).replace("J", "") + nr
									: res.staffel);
			return res;
		} catch (Exception e) {
			HSGApp.logger.error("Import für Spiel '" + l + "' fehlgeschlagen:" + e.getMessage());
			return null;
		}
	}

	public HSGInterval getDienstSperrenZeitraum() {
		return new HSGInterval(
				getZeit().minus(isHeimspiel() ? SPERRSTD_VORLAUF_HEIMSPIEL
						: SPERRSTD_VORLAUF_AUSWÄRTSSPIEL, ChronoUnit.HOURS),
				getZeit().plus(isHeimspiel() ? SPERRSTD_NACHLAUF_HEIMSPIEL
						: SPERRSTD_NACHLAUF_AUSWÄRTSSPIEL, ChronoUnit.HOURS));
	}

	@Override
	public int compareTo(final Game o) {
		int res = -Boolean.compare(heimspiel, o.heimspiel);
		if (res == 0) {
			res = date.compareTo(o.date);
			if (res == 0) {
				return zeit.compareTo(o.zeit);
			}
		}
		return res;
	}

	@Override
	public String toString() {
		return date.toddMMyyyy() + ", " + zeit + " " + staffel + ": " + heim + "-" + gast;
	}

	public String toCSV() {
		return String.join(";", zeit.toString(), staffel, heim, gast);
	}

}
