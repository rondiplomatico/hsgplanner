package dw.tools.hsg;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * HSGDateTime is a Timestamp Wrapper for PBK. A DateTime is "YYYY-MM-HH
 * hh:mm:ss.sss". Usage: for all Dates which needs year, month and day, hour,
 * miute and day.
 *
 * @author mattist
 * @since 01.08.2016
 */
public class HSGDateTime implements Serializable, Comparable<HSGDateTime> {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	private static final ZoneId SYSTEM_DEFAULT = ZoneId.systemDefault();

	/** The Timestamp. */
	private final LocalDateTime datetime;

	/**
	 * Formatter for parsing {@link HSGDateTime} from ISO format.
	 */
	private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	/**
	 * Formatter for printing {@link HSGDateTime} into "yyyyMMddHHmmss" format
	 */
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

	/**
	 * Formatter for printing {@link HSGDateTime} into "yyyyMMddHHmm" format
	 */
	private static final DateTimeFormatter PBK_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

	/**
	 * Creates a new {@link HSGDateTime} instance for given year, month, day, hour,
	 * minute and second.
	 *
	 * @param year   the year
	 * @param month  the month
	 * @param day    the day
	 * @param hour   the hour
	 * @param minute the minute
	 * @param second the second
	 */
	public HSGDateTime(final int year, final int month, final int day, final int hour, final int minute,
			final int second) {
		datetime = LocalDateTime.of(year, month, day, hour, minute, second);
	}

	/**
	 * Creates a HSGDateTime from an SQL Timestamp.
	 *
	 * @param timestamp the timestamp
	 */
	public HSGDateTime(final Timestamp timestamp) {
		datetime = timestamp.toLocalDateTime();
	}

	/**
	 * Creates a new {@link HSGDateTime} instance from a java.util.Date
	 *
	 * @param date the date
	 */
	public HSGDateTime(final java.util.Date date) {
		datetime = Instant.ofEpochMilli(date.getTime()).atZone(SYSTEM_DEFAULT).toLocalDateTime();
	}

	/**
	 * Creates a HSGDateTime from a timestamp.
	 *
	 * @param time the time
	 */
	public HSGDateTime(final long time) {
		datetime = Instant.ofEpochMilli(time).atZone(SYSTEM_DEFAULT).toLocalDateTime();
	}

	/**
	 * Instantiates a new HSGDateTime from a LocalDateTime.
	 *
	 * @param datetime the datetime
	 */
	public HSGDateTime(final LocalDateTime datetime) {
		this.datetime = datetime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return datetime.toString();
	}

	/**
	 * See {@link Timestamp#toLocalDate()}
	 *
	 * @return the local date
	 */
	public LocalDateTime toLocalDate() {
		return datetime;
	}

	/**
	 * See {@link Timestamp#toInstant()}
	 *
	 * @return the instant
	 */
	public Instant toInstant() {
		return datetime.atZone(SYSTEM_DEFAULT).toInstant();
	}

	/**
	 * See {@link Timestamp#getTime()}
	 *
	 * @return the time
	 */
	public long getTime() {
		return toInstant().toEpochMilli();
	}

	/**
	 * See {@link Timestamp#before()}
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean before(final HSGDateTime when) {
		return datetime.isBefore(when.datetime);
	}

	/**
	 * Checks if this date is before or equal to the given date.
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean beforeOrEquals(final HSGDateTime when) {
		return before(when) || equals(when);
	}

	/**
	 * Checks if this date is after or equal to the given date.
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean afterOrEquals(final HSGDateTime when) {
		return after(when) || equals(when);
	}

	/**
	 * See {@link Timestamp#after()}
	 *
	 * @param when the when
	 * @return true, if successful
	 */
	public boolean after(final HSGDateTime when) {
		return datetime.isAfter(when.datetime);
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
			return datetime.equals(((HSGDateTime) obj).datetime);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return datetime.hashCode();
	}

	/**
	 * See {@link Date#valueOf(LocalDateTime)}.
	 *
	 * @param datetime the datetime
	 * @return the HSGDateTime
	 */
	public static HSGDateTime valueOf(final LocalDateTime datetime) {
		return new HSGDateTime(datetime);
	}

	/**
	 * Gets the real Timestamp.
	 *
	 * @return the real Date
	 */
	public java.sql.Timestamp getInnerTimestamp() {
		return Timestamp.valueOf(datetime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final HSGDateTime o) {
		return datetime.compareTo(o.datetime);
	}

	/**
	 * Returns a HSGDateTime of the current date-time from the system clock in the
	 * default time-zone
	 *
	 * @return the HSGDateTime
	 */
	public static HSGDateTime now() {
		return HSGDateTime.valueOf(LocalDateTime.now());
	}

	/**
	 * New HSGDateTime from an ISO formatted date, without the letter 'T' as
	 * delimiter. ISO format is e.g. '2017-12-03T10:15:30'.
	 *
	 * @param date the date
	 * @return the HSGDateTime
	 */
	public static HSGDateTime fromISO(final String date) {
		return HSGDateTime.valueOf(LocalDateTime.from(ISO_FORMATTER.parse(date)));
	}

	/**
	 * New HSGDateTime from a PBK Timestamp formatted date. PBK Timestamp formatted
	 * date is "yyyyMMddHHmm" format.
	 *
	 * @param date the date
	 * @return the HSGDateTime
	 */
	public static HSGDateTime fromPBKDateTimeFormat(final String date) {
		return HSGDateTime.valueOf(LocalDateTime.from(PBK_DATE_TIME_FORMATTER.parse(date)));
	}

	/**
	 * New HSGDateTime from a Timestamp formatted date. Timestamp formatted date is
	 * "yyyyMMddHHmmss" format.
	 *
	 * @param date the date
	 * @return the HSGDateTime
	 */
	public static HSGDateTime fromDateTimeFormat(final String date) {
		return HSGDateTime.valueOf(LocalDateTime.from(DATE_TIME_FORMATTER.parse(date)));
	}

	/**
	 * Formats the date in the timestamp into "yyyy-MM-dd HH:mm:ss" format.
	 *
	 * @return the formatted date time
	 */
	public String toISOFormat() {
		return toLocalDate().format(ISO_FORMATTER);
	}

	/**
	 * Formats the date in the timestamp into "yyyyMMddHHmmss" format.
	 *
	 * @return the formatted date time
	 */
	public String toDateTimeFormat() {
		return toLocalDate().format(DATE_TIME_FORMATTER);
	}

	/**
	 * Converts HSGDateTime to HSGDate.
	 *
	 * @return the HSGDate value
	 */
	public HSGDate toPBKDate() {
		return new HSGDate(datetime.getYear(), datetime.getMonthValue(), datetime.getDayOfMonth());
	}

	/**
	 * Format with a pattern.
	 *
	 * @param pattern the pattern
	 * @return the string
	 */
	public String format(final DateTimeFormatter pattern) {
		return toLocalDate().format(pattern);
	}
}
