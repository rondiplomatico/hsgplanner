/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    HSGInterval.java
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
package dw.tools.hsg.data;

import java.io.Serializable;
import java.time.LocalTime;

import lombok.Data;
import lombok.EqualsAndHashCode;
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
@RequiredArgsConstructor
public class HSGInterval implements Serializable {

	private static final long serialVersionUID = 8533515473444363858L;

	public static final HSGInterval MAXMIN = new HSGInterval(LocalTime.MAX, LocalTime.MIN);
	public static final HSGInterval ALL_DAY = new HSGInterval(LocalTime.MIN, LocalTime.MAX);
	public static final HSGInterval BIS_16 = new HSGInterval(LocalTime.MIN, LocalTime.of(16, 00));
	public static final HSGInterval BIS_18 = new HSGInterval(LocalTime.MIN, LocalTime.of(18, 00));
	public static final HSGInterval BIS_20 = new HSGInterval(LocalTime.MIN, LocalTime.of(20, 00));
	public static final HSGInterval EMPTY = new HSGInterval(LocalTime.MIN, LocalTime.MIN);

	private final LocalTime start;
	private final LocalTime end;

	public HSGInterval merge(final HSGInterval other) {
		return new HSGInterval(start.isBefore(other.start) ? start : other.start,
				end.isAfter(other.end) ? end : other.end);
	}

	public HSGInterval stretch(final LocalTime to) {
		return new HSGInterval(to.isBefore(start) ? to : start, end.isBefore(to) ? to : end);
	}

	public int dauerInMin() {
		return (end.getHour() - start.getHour()) * 60 + end.getMinute() - start.getMinute();
	}

	@Override
	public String toString() {
		return start + " - " + end;
	}

	public boolean isEmpty() {
		return start == null || end == null || start.equals(end) || end.isBefore(start);
	}

	public boolean contains(final LocalTime date) {
		return start.equals(date) || end.equals(date) || start.isBefore(date) && end.isAfter(date);
	}

	public boolean contains(final HSGInterval other) {
		return (start.equals(other.start) || start.isBefore(other.start))
				&& (end.equals(other.end) || end.isAfter(other.end));
	}

	public boolean intersects(final HSGInterval other) {
		return contains(other.getStart()) || contains(other.getEnd()) || other.contains(getStart());
	}

	public int minuteDistance(final LocalTime to) {
		if (contains(to)) {
			return 0;
		}
		if (to.compareTo(getStart()) < 0) {
			return (getStart().toSecondOfDay() - to.toSecondOfDay()) / 60;
		}
		return (to.toSecondOfDay() - getEnd().toSecondOfDay()) / 60;
	}

	public int minuteDistance(final HSGInterval other) {
		return Math.min(minuteDistance(other.getStart()), minuteDistance(other.getEnd()));
	}

	public int compareTo(final HSGInterval o) {
		if (getStart() == null || getEnd() == null || o == null) {
			return -1;
		}
		int start = getStart().compareTo(o.getStart());
		if (start == 0) {
			return getEnd().compareTo(o.getEnd());
		}
		return start;
	}

}
