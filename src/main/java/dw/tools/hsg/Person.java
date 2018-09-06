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
@NoArgsConstructor(access=AccessLevel.PRIVATE)
@AllArgsConstructor
public class Person implements Serializable {

    private static final long serialVersionUID = 3556391185364393215L;

    private String name;
    private String teamId;
    private int gearbeitetM;

    public static Person parse(final String line) {
        String[] elems = line.split(";");
        Person res = new Person();
        res.name = elems[0];
        res.teamId = elems[2];
        res.gearbeitetM = (int) Math.round(Double.parseDouble(elems[1].replace(",", ".")) * 60);
        return res;
    }

}
