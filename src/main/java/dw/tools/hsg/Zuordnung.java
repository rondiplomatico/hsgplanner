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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * TODO Replace with class description.
 *
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
@Getter
@ToString
@EqualsAndHashCode
public class Zuordnung implements Serializable {

    private static final long serialVersionUID = 6500267604562155991L;
    private static int ID = 0;

    private final Person person;
    private final Dienst dienst;
    // private final UUID id;
    private final int id;

    public String varName() {
        // return id.toString();
        // return "V"+id;
        return id + ":" + person.getName() + "-" + dienst.zeit;
    }

    public Zuordnung(final Person p, final Dienst d) {
        person = p;
        dienst = d;
        // id = UUID.randomUUID();
        id = ID++;
    }
}
