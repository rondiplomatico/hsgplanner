/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Spieltag.java
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

import lombok.Data;
import scala.Tuple2;

/**
 * TODO Replace with class description.
 *
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
@Data
public class Spieltag implements Serializable {

    private static final long serialVersionUID = -2000124935112267942L;

    HSGDate datum;
    LocalTime erstesSpiel;
    LocalTime letztesSpiel;

    List<Tuple2<Team, HSGInterval>> auswärtsSpielZeiten = new ArrayList<>();
    List<Tuple2<Person, HSGInterval>> blockiertePersonen = new ArrayList<>();
    List<Dienst> dienste = new ArrayList<>();

}
