/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    SOS.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     07.09.2018
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2018, all rights reserved
 * _____________________________________________________________________________
 */
package net.sf.javailp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 *
 * @version $Revision$
 * @author wirtzd
 * @since 07.09.2018
 */
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public class SOS {

    @RequiredArgsConstructor
    @Getter
    public static enum SOSType {
        SOS1(1),
        SOS2(2),
        SOS3(3),
        SOS4(4),
        SOS5(5);

        private final int num;
    }

    private String name;
    private final Linear lhs;
    private final SOSType type;

    public int size() {
        return lhs.size();
    }

    @Override
    public String toString() {
        return type + ": " + lhs + " = 1";
    }

}
