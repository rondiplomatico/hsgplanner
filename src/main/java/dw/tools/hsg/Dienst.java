/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Dienst.java
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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * TODO Replace with class description.
 *
 * @version $Revision$
 * @author wirtzd
 * @since 05.09.2018
 */
@Data
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
public class Dienst implements Serializable {

    private static final long serialVersionUID = 2386602455909060467L;

    @RequiredArgsConstructor
    public enum Typ {
        Verkauf('V', 2, 2, 4, new int[] { 5, 6, 7, 8 }),
        Kasse('K', 1, 2, 1, new int[] { 3, 4, 5, 6 }),
        Aufsicht('A', 1, 2, 6, new int[] { 8, 9, 10, 11 });

        @Getter private final char kurz;
        @Getter private final int personen;
        @Getter private final int vorlaufHS;
        @Getter private final int nachlaufHS;
        @Getter private final int[] timesHS;

        public Dienst newDienst() {
            Dienst res = new Dienst();
            res.typ = this;
            return res;
        }
    }

    HSGDate datum;
    HSGInterval zeit;
    Typ typ;

    @Override
    public String toString() {
        return typ.getKurz() + " " + zeit;
    }

}
