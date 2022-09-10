/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    HSGIntervalTest.java
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

import static org.junit.Assert.assertEquals;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import dw.tools.hsg.data.HSGInterval;

/**
 * @author wirtzd
 * @since 11.09.2018
 */
public class HSGIntervalTest {

    @Test
    public void testMinuteDistance() {
        HSGInterval a = new HSGInterval(LocalTime.of(12, 00), LocalTime.of(15, 00));

        assertEquals(0, a.minuteDistance(LocalTime.of(12, 00)));
        assertEquals(0, a.minuteDistance(LocalTime.of(15, 00)));
        assertEquals(0, a.minuteDistance(LocalTime.of(13, 00)));

        assertEquals(30, a.minuteDistance(LocalTime.of(11, 30)));
        assertEquals(200, a.minuteDistance(LocalTime.of(12, 00).minus(200L, ChronoUnit.MINUTES)));
        assertEquals(61, a.minuteDistance(LocalTime.of(16, 1)));
        assertEquals(1, a.minuteDistance(LocalTime.of(15, 1)));
    }

}
