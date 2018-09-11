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

    private String name;
    private String shortName;
    private Team team;
    private int gearbeitetM;
    private boolean aufsicht;
    private String trainerVon = null;

    public Person(final String name, final Team team, final int worked,
                    final boolean aufsicht, final String trainerVon) {
        this(name, createShort(name), team, worked, aufsicht, trainerVon);
    }

    public Person(final String name, final String teamId, final int worked,
                    final boolean aufsicht, final String trainerVon) {
        this(name, createShort(name), new Team(teamId), worked, aufsicht, trainerVon);
    }

    public Person(final String name, final String teamId, final int worked) {
        this(name, new Team(teamId), worked, false, null);
    }

    /*
     * Jemand ist zulässig für Arbeitsdienste wenn ihr Team in der Liste der arbeitenden Teams ist oder sie als Aufsicht
     * markiert ist.
     */
    public boolean isValid() {
        return team.mayWork() || aufsicht;
    }

    public boolean mayWorkAt(final Dienst d) {
        return Typ.Aufsicht == d.getTyp() && isAufsicht()
                        || team.mayWorkAt(d) && Typ.Aufsicht != d.getTyp() && !isAufsicht();
    }

    private static String createShort(final String name) {
        if (name.contains(" ")) {
            String[] parts = name.split(" ");
            return parts[0].substring(0, 1) + parts[1].substring(0, 1);
        }
        return name;
    }

    public static Person parse(final String line) {
        String[] elems = line.split(";");
        Team team = new Team(elems[1]);
        boolean aufsicht = !Strings.isNullOrEmpty(elems[3]) && AUFSICHT_MARKER.equalsIgnoreCase(elems[3]);
        return new Person(elems[0], team,
                          aufsicht ? 0 : (int) Math.round(Double.parseDouble(elems[4].replace(",", ".")) * 60),
                          aufsicht, elems[2]);
    }

    @Override
    public String toString() {
        return name + "@" + team.getId() + (aufsicht ? "/!" : "");
    }

}
