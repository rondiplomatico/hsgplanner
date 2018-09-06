/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    IntervalDisjoiner.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     25.01.2017
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2017, all rights reserved
 * _____________________________________________________________________________
 */
package dw.tools.hsg;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Helper class to compute disjoined validities from a set of validity atoms or intervals
 *
 * @author wirtzd
 * @since 25.01.2017
 */
public final class IntervalDisjoiner {

    private IntervalDisjoiner() {
        super();
    }

    /*
     * Package vis for unit testing.
     */
    static Map<HSGInterval, List<HSGInterval>> disjoin(final Collection<HSGInterval> intervals) {
        Set<LocalTime> dateSet = new HashSet<>();
        for (HSGInterval i : intervals) {
            dateSet.add(i.getStart());
            dateSet.add(i.getEnd());//.plus(30L, ChronoUnit.MINUTES));
        }

        List<LocalTime> dates = new ArrayList<>(dateSet);
        Collections.sort(dates);
        Map<HSGInterval, List<HSGInterval>> res = new TreeMap<>((a, b) -> a.compareTo(b));
        /*
         * With this method we create all consecutive intervals between the earliest start
         * and the latest end, even though the passed list might have gaps.
         * This is why we check if there are any originating intervals for the fine
         * interval and skip those without any
         */
        for (int i = 0; i < dates.size() - 1; i++) {
            HSGInterval fine = new HSGInterval(dates.get(i), dates.get(i + 1));//.minus(30L, ChronoUnit.MINUTES));
            List<HSGInterval> origins = new ArrayList<>(intervals.stream()
                                                                 .filter(v -> v.contains(fine))
                                                                 .collect(Collectors.toList()));
            if (!origins.isEmpty()) {
                Collections.sort(origins, (a, b) -> a.compareTo(b));
                res.put(fine, origins);
            }
        }
        return res;
    }
}
