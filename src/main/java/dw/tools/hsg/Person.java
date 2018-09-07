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
	private static final String AUFSICHT_TEAM_ID = "HA";

	private String name;
	private String shortName;
	private String teamId;
	private int gearbeitetM;
	private boolean aufsicht;

	public Person(final String name, final String teamId, final int worked, final boolean aufsicht) {
		this(name, createShort(name), teamId, worked, aufsicht);
	}

	public Person(final String name, final String teamId, final int worked) {
		this(name, teamId, worked, false);
	}

	private static String createShort(String name) {
		if (name.contains(" ")) {
			String[] parts = name.split(" ");
			return parts[0].substring(0, 1) + parts[1].substring(0, 1);
		}
		return name;
	}

	public static Person parse(final String line) {
		String[] elems = line.split(";");
		boolean aufsicht = AUFSICHT_TEAM_ID.equals(elems[2])
				|| elems.length > 3 && AUFSICHT_MARKER.equalsIgnoreCase(elems[3]);
		return new Person(elems[0], elems[2],
				aufsicht ? 0 : (int) Math.round(Double.parseDouble(elems[1].replace(",", ".")) * 60), aufsicht);
	}
}
