package dw.tools.hsg;

import java.io.Serializable;
import java.sql.Date;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

import org.apache.spark.util.KnownSizeEstimation;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * The only Date class which should be used in PBK/Hadoop. A Date is
 * "YYYY-MM-HH". Usage: for all Dates which only needs year, month and day.
 * Ranges: from 0001-01-01 (no 0000-01-01 due to limits in Localdate) to
 * 9999-12-31
 *
 * Wraps a java.sql.Date with useful methods. Deprecated methods from
 * java.sql.Date oder java.util.Date are no longer delegated.
 *
 * @author mattist
 * @since 28.04.2016
 */
public class HSGDate implements Serializable, Comparable<HSGDate>, KryoSerializable, KnownSizeEstimation {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	private static final ZoneId SYSTEM_DEFAULT = ZoneId.systemDefault();

	static {
		LocalDate now = LocalDate.now();
		TODAY = valueOf(LocalDate.of(now.getYear(), now.getMonth(), now.getDayOfMonth()));
		FIRST_DAY_OF_CURRENT_MONTH = new HSGDate(now.getYear(), now.getMonthValue(), 1);
	}

	/**
	 * Formatter for printing {@link HSGDate} into "yyyyMM" string format
	 */
	private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");

	private static final DateTimeFormatter DD_MM_YY_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");

	public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");

	/**
	 * The HSGDate of today with only year, month and day set.
	 */
	public static final HSGDate TODAY;

	/** The Constant FIRST_DAY_OF_CURRENT_MONTH. */
	public static final HSGDate FIRST_DAY_OF_CURRENT_MONTH;

	/** The Constant MAX_VALUE. */
	public static final HSGDate MAX_VALUE = new HSGDate(9999, 12, 31);

	/** The Constant MIN_VALUE. */
	public static final HSGDate MIN_VALUE = new HSGDate(1, 1, 1);

	private static final long MSEC_TO_EPOCH_DAY = 1000 * 24 * 60 * 60L;

	/** The date. */
	private LocalDate date;

	/**
	 * Creates a new {@link HSGDate} instance for given year, month and day.
	 *
	 * @param year  The year
	 * @param month The month
	 * @param day   The day
	 */
	public HSGDate(final int year, final int month, final int day) {
		date = LocalDate.of(year, month, day);
	}

	/**
	 * Creates a HSGDate from an SQL Date.
	 *
	 * @param date the date
	 */
	@SuppressWarnings("findbugs:EI_EXPOSE_REP2")
	public HSGDate(final java.sql.Date date) {
		this.date = Instant.ofEpochMilli(date.getTime()).atZone(SYSTEM_DEFAULT).toLocalDate();
	}

	/**
	 * Creates a new {@link HSGDate} instance from a java.util.Date
	 *
	 * @param date the date
	 */
	public HSGDate(final java.util.Date date) {
		this.date = Instant.ofEpochMilli(date.getTime()).atZone(SYSTEM_DEFAULT).toLocalDate();
	}

	/**
	 * Creates a HSGDate from a timestamp.
	 *
	 * @param time the time
	 */
	public HSGDate(final long time) {
		date = LocalDate.ofEpochDay(time / MSEC_TO_EPOCH_DAY);
	}

	/**
	 * Creates a new {@link HSGDate} instance from a date string formatted as
	 * DD_MM_YYYY_FORMATTER.
	 *
	 * @param date the date
	 */
	public HSGDate(final String date) {
		this.date = LocalDate.from(DD_MM_YY_FORMATTER.parse(date));
	}

	/**
	 * Instantiates a new PBK date from a local date
	 *
	 * @param date the date
	 */
	public HSGDate(final LocalDate date) {
		this.date = date;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return date.toString();
	}

	/**
	 * See {@link java.sql.Date#toLocalDate()}
	 *
	 * @return the local date
	 */
	public LocalDate toLocalDate() {
		return date;
	}

	/**
	 * See {@link java.sql.Date#getTime()}
	 *
	 * @return the time
	 */
	public long getTime() {
		return date.toEpochDay() * MSEC_TO_EPOCH_DAY;
	}

	public boolean isSaturday() {
		return date.getDayOfWeek() == DayOfWeek.SATURDAY;
	}

	public boolean isSunday() {
		return date.getDayOfWeek() == DayOfWeek.SUNDAY;
	}

	public HSGDate getWeekend() {
		return isSaturday() ? new HSGDate(date.minusDays(1)) : (isSunday() ? new HSGDate(date.minusDays(2)) : null);
	}

	/**
	 * See {@link java.sql.Date#before()}
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean before(final HSGDate when) {
		return date.isBefore(when.date);
	}

	/**
	 * Checks if this date is before or equal to the given date.
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean beforeOrEquals(final HSGDate when) {
		return before(when) || equals(when);
	}

	/**
	 * Checks if this date is after or equal to the given date.
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean afterOrEquals(final HSGDate when) {
		return after(when) || equals(when);
	}

	/**
	 * See {@link java.sql.Date#after()}
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean after(final HSGDate when) {
		return date.isAfter(when.date);
	}

	/**
	 * Returns the minimum of this and the specified date.
	 *
	 * @param other the other
	 * @return the earlier of the two dates
	 */
	public HSGDate min(final HSGDate other) {
		return before(other) ? this : other;
	}

	/**
	 * Returns the maximum of this and the specified date.
	 *
	 * @param other the other
	 * @return the later of the two dates
	 */
	public HSGDate max(final HSGDate other) {
		return after(other) ? this : other;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() == obj.getClass()) {
			return date.equals(((HSGDate) obj).date);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return date.hashCode();
	}

	/**
	 * See {@link Date#valueOf(LocalDate)}.
	 *
	 * @param date the date
	 * @return the PBK date
	 */
	public static HSGDate valueOf(final LocalDate date) {
		return new HSGDate(date);
	}

	/**
	 * Gets the real java.util.Date.
	 *
	 * @return the real Date
	 */
	public java.util.Date getInnerDate() {
		return java.util.Date.from(date.atStartOfDay().atZone(SYSTEM_DEFAULT).toInstant());
	}

	/**
	 * Gets the inner sql date.
	 *
	 * @return the inner sql date
	 */
	public java.sql.Date getInnerSqlDate() {
		return java.sql.Date.valueOf(date);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final HSGDate o) {
		return date.compareTo(o.date);
	}

	/**
	 * Returns the first date of the year associated with this instance.
	 *
	 * @return the first date of year
	 */
	public HSGDate getFirstDateOfYear() {
		return new HSGDate(LocalDate.ofYearDay(date.getYear(), 1));
	}

	/**
	 * New PBK Date from an ISO formatted date. * ISO format is e.g. '2011-12-03' or
	 * '2011-12-03+01:00'. see {@link DateTimeFormatter.ISO_DATE}
	 *
	 * @param date the date
	 * @return the PBK date
	 */
	public static HSGDate fromISO(final String date) {
		return new HSGDate(LocalDate.from(DateTimeFormatter.ISO_DATE.parse(date)));
	}

	/**
	 * New PBK Date from an BASIC_ISO formatted date. Formats or parses a date
	 * without an offset, such as '20111203'.
	 *
	 * @param date the date
	 * @return the PBK date
	 */
	public static HSGDate fromBasicISO(final String date) {
		return new HSGDate(LocalDate.of(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(4, 6)),
				Integer.parseInt(date.substring(6, 8))));
	}

	/**
	 * 12.05.18
	 * 
	 * @param date
	 * @return
	 */
	public static HSGDate fromDDMMYY(final String date) {
		return new HSGDate(LocalDate.of(2000 + Integer.parseInt(date.substring(6, 8)),
				Integer.parseInt(date.substring(3, 5)), Integer.parseInt(date.substring(0, 2))));
	}

	/**
	 * New PBK Date from an BASIC_ISO formatted date. Formats or parses a date
	 * without an offset, such as '20111203'.
	 *
	 * @param date the date
	 * @return the PBK date
	 */
	public static HSGDate fromBasicISONullable(final String date) {
		if (date != null && !date.isEmpty()) {
			return new HSGDate(LocalDate.from(DateTimeFormatter.BASIC_ISO_DATE.parse(date)));
		}
		return null;
	}

	/**
	 * Formats the date into "yyyyMMdd" format.
	 *
	 * @return the formatted date
	 */
	public String toBasicISO() {
		return date.format(DateTimeFormatter.BASIC_ISO_DATE);
	}

	/**
	 * To integer yyyyMMdd. year * 10.000 + month * 100 + day
	 *
	 * @return the int
	 */
	public int toIntegeryyyyMMdd() {
		return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
	}

	/**
	 * To integer yyyyMM. year * 100 + month
	 *
	 * @return the int
	 */
	public int toIntegeryyyyMM() {
		return date.getYear() * 100 + date.getMonthValue();
	}

	/**
	 * To dd.MM.yyyy
	 *
	 * @return the string
	 */
	public String toddMMyyyy() {
		return date.format(DD_MM_YY_FORMATTER);
	}

	/**
	 * Formats the date into "yyyyMM" format.
	 *
	 * @return the formatted date
	 */
	public String toMonthFormat() {
		return date.format(MONTH_FORMATTER);
	}

	/**
	 *
	 * Calculates a day relative to caller.
	 *
	 * @param numDay if positive number then returns the caller + param day (after)
	 *               if negative number then returns the caller - param day (before)
	 * @return the new day
	 */
	public HSGDate nextDay(final int numDay) {
		return new HSGDate(date.plusDays(numDay));
	}

	/**
	 *
	 * Adds numMonths to the current date.
	 *
	 * numMonths can also be negative.
	 *
	 *
	 * @param numMonths
	 * @return
	 */
	public HSGDate addMonths(final int numMonths) {
		return valueOf(numMonths < 0 ? date.minusMonths(-numMonths) : date.plusMonths(numMonths));
	}

	/**
	 * Get the first day of the month.
	 *
	 * @return the first day of month
	 */
	public HSGDate getFirstDayOfMonth() {
		return valueOf(date.withDayOfMonth(1));
	}

	/**
	 * Get the 15th day of the month.
	 *
	 * @return the 15th day of month
	 */
	public HSGDate getMiddleOfMonth() {
		return valueOf(date.withDayOfMonth(15));
	}

	/**
	 * Get the last day of the month.
	 *
	 * @return the last day of month
	 */
	public HSGDate getLastDayOfMonth() {
		return valueOf(date.withDayOfMonth(lengthOfMonth()));
	}

	/**
	 * Get the first day of the week.
	 *
	 * @return the first day of week
	 */
	public HSGDate getFirstDayOfWeek() {
		return getDayOfWeek(DayOfWeek.MONDAY);
	}

	/**
	 * Get the first day of the week.
	 *
	 *
	 * @param dayOfWeek the day of the week that should be returned
	 * @return the first day of week
	 */
	public HSGDate getDayOfWeek(final DayOfWeek dayOfWeek) {
		return valueOf(date.with(ChronoField.DAY_OF_WEEK, dayOfWeek.getValue()));
	}

	/**
	 * Calculates the number of days between this <code>HSGDate</code> and the given
	 * other <code>HSGDate</code>.
	 *
	 * @param other the other <code>HSGDate</code>
	 * @return number of days, negative if <code>other</code> is before
	 *         <code>this</code>, and positive if <code>other</code> is after
	 *         <code>this</code>.
	 */
	public long getDaysBetweenThisAnd(final HSGDate other) {
		return other.date.toEpochDay() - date.toEpochDay();
	}

	/**
	 * Gets the number of months between the month of this date and the specified
	 * date. May return zero if there is no change of month between the two dates.
	 *
	 * @param other the other
	 * @return the number of month changes between this and the specified month
	 */
	public int getMonthsBetweenThisAnd(final HSGDate other) {
		return other.date.getMonthValue() - date.getMonthValue() + 12 * (other.date.getYear() - date.getYear());
	}

	/**
	 * Returns the number of week days (not saturday, sunday) between the two given
	 * dates (including both dates). No distinction is made between work days and
	 * public holidays, however.
	 *
	 * This method is lenient with respect of the fields finer than day of week
	 * within the two given dates, i.e.
	 *
	 * @param toDate the end of the range of days within which to count the week
	 *               days (must be the same day or after <code>toDate</code>).
	 * @return the number of week days between both dates, including boundaries.
	 *         Return -999 if one of the boundaries is null or if this date is after
	 *         the toDate.
	 */
	public int weekDayDifference(final HSGDate toDate) {
		if (toDate == null || compareTo(toDate) > 0) {
			return -999;
		}
		LocalDate fromLocalDate = date;
		LocalDate toLocalDate = toDate.date;
		int count = 0;
		while (fromLocalDate.compareTo(toLocalDate) <= 0) {
			if (fromLocalDate.getDayOfWeek() != DayOfWeek.SATURDAY
					&& fromLocalDate.getDayOfWeek() != DayOfWeek.SUNDAY) {
				count++;
			}
			fromLocalDate = fromLocalDate.plusDays(1);
		}
		return count;
	}

	/**
	 * Adds a given number of the work days to the specified date.
	 *
	 * The number can also be negative.
	 *
	 * @param days the days
	 * @return the PBK date
	 */
	public HSGDate addWorkdays(final int days) {
		if (days == 0) {
			return new HSGDate(date);
		}
		LocalDate result = date;
		int movedDays = 0;
		while (movedDays < Math.abs(days)) {
			result = days < 0 ? result.minusDays(1) : result.plusDays(1);
			if (!(result.getDayOfWeek() == DayOfWeek.SATURDAY || result.getDayOfWeek() == DayOfWeek.SUNDAY)) {
				++movedDays;
			}
		}
		return new HSGDate(result);
	}

	/**
	 * Adds the specified amount of days to the current date.
	 *
	 * Value may be negative.
	 *
	 * @param days the days to add, may be negative
	 * @return the PBK date
	 */
	public HSGDate addDays(final int days) {
		return new HSGDate(date.plusDays(days));
	}

	/**
	 * Returns the length of this date's month.
	 *
	 * @return the length of the month
	 */
	public int lengthOfMonth() {
		return date.lengthOfMonth();
	}

	@Override
	public long estimatedSize() {
		return 10L + 8;
	}

	@Override
	public void write(final Kryo kryo, final Output output) {
		output.writeLong(date == null ? 0 : date.toEpochDay());
	}

	@Override
	public void read(final Kryo kryo, final Input input) {
		long epochDay = input.readLong();
		date = LocalDate.ofEpochDay(epochDay);
	}
}
