/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Zuordnung.java
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
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import dw.tools.hsg.Dienst.Typ;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
@Data
@EqualsAndHashCode
@RequiredArgsConstructor
public class Zuordnung implements Serializable {

	private static final long serialVersionUID = 6500267604562155991L;
	private static int ID = 0;

	private final Person person;
	private final Dienst dienst;
	private final int nr;
	private final int id;
	private boolean fixed = false;

	public String varName() {
		return person.getShortName() + "@" + dienst.getTyp().getKurz() + dienst.zeit + "/" + nr + "/" + id;
	}

	public Zuordnung(final Person p, final Dienst d, final int nr) {
		person = p;
		dienst = d;
		this.nr = nr;
		// id = UUID.randomUUID();
		id = ID++;
	}

	public Zuordnung(final Person p, final Dienst d) {
		this(p, d, 0);
	}

	@Override
	public String toString() {
		return varName();
	}

	public static List<Zuordnung> read(String line) {
		String[] elems = line.split(";");
		Team team = Team.valueOf(elems[4]);
		Person p = new Person(elems[5], team, 0, false, null);
		Dienst d = new Dienst(HSGDate.fromDDMMYY(elems[0].substring(4)),
				new HSGInterval(LocalTime.parse(elems[1], HSGDate.TIME_FORMATTER),
						LocalTime.parse(elems[2], HSGDate.TIME_FORMATTER)),
				Typ.valueOf(elems[3]));
		List<Zuordnung> res = new ArrayList<>();
		res.add(new Zuordnung(p, d));
		if (elems.length > 6) {
			res.add(new Zuordnung(new Person(elems[6], team, 0, false, null), d, 1));
		}
		return res;
	}
}
