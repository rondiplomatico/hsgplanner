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

import java.io.Serializable;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @version $Revision$
 * @author wirtzd
 * @since 11.09.2018
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class Team implements Serializable {

	private static final long serialVersionUID = -2684012606850895772L;

	public static final Map<String, HSGInterval> WORKING_TEAMS = teamTimes();
	public static final List<String> mitKasse = new ArrayList<>(Arrays.asList("F1", "F2", "M1"));
	public static final Map<String, Double> leistungsFaktoren;
	static {
		leistungsFaktoren = new HashMap<>();
		leistungsFaktoren.put("mA1", 0.8);
		leistungsFaktoren.put("wA1", 0.8);
		leistungsFaktoren.put("mA2", 0.8);
		leistungsFaktoren.put("wA2", 0.8);
		leistungsFaktoren.put("mB1", 0.6);
		leistungsFaktoren.put("wB1", 0.6);
		leistungsFaktoren.put("mB2", 0.6);
		leistungsFaktoren.put("wB2", 0.6);
	}

	private final String id;

	private static Map<String, HSGInterval> teamTimes() {
		Map<String, HSGInterval> res = new HashMap<>();
		Arrays.asList("M1", "M2", "M3", "M4", "M5", "F1", "F2", "F3", "F4").forEach(
				s -> res.put(s, HSGInterval.ALL_DAY));
		Arrays.asList("mA1", "mA2", "wA1", "wA2").forEach(
				s -> res.put(s, new HSGInterval(LocalTime.MIN, LocalTime.of(17, 00))));
		Arrays.asList("mB1", "mB2", "wB1", "wB2").forEach(
				s -> res.put(s, new HSGInterval(LocalTime.MIN, LocalTime.of(15, 00))));
		return res;
	}

	public double leistungsFaktor() {
		return leistungsFaktoren.getOrDefault(id, 1.0);
	}

	public boolean mitKasse() {
		return mitKasse.contains(id);
	}

	public boolean mayWorkAt(final Dienst d) {
		return mayWork() && WORKING_TEAMS.get(id).contains(d.getZeit());
	}

	public boolean mayWork() {
		return WORKING_TEAMS.containsKey(id);
	}

	@Override
	public String toString() {
		return id;
	}

	public boolean isJugend() {
		return !id.startsWith("M") && !id.startsWith("F");
	}

}
