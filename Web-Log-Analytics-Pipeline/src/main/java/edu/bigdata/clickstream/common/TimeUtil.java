package edu.bigdata.clickstream.common;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * TimeUtil provides allocation-free, regex-free timestamp validation and parsing
 * per SRS §3.3.3 Rule 3 and CLAUDE.md §10.
 *
 * Supported format: "yyyy-MM-dd HH:mm:ss UTC"
 */
public final class TimeUtil {

    private static final int[] DAYS_IN_MONTH = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };

    private TimeUtil() {
        // Utility class
    }

    /**
     * Fast check if a character is an ASCII digit ('0'-'9').
     */
    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    /**
     * Determines whether year is a leap year in the proleptic Gregorian calendar.
     */
    public static boolean isLeapYear(int year) {
        return ((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0);
    }

    /**
     * Validates timestamp string per SRS Rule 3:
     * - Length >= 19
     * - Matches yyyy-MM-dd HH:mm:ss
     * - Month in 1-12, day valid for month (including leap year February)
     * - Hour in 0-23, minute in 0-59, second in 0-59
     */
    public static boolean isValidTimestamp(String s) {
        if (s == null || s.length() < 19) {
            return false;
        }

        // Year: chars 0..3
        for (int i = 0; i < 4; i++) {
            if (!isDigit(s.charAt(i))) return false;
        }
        if (s.charAt(4) != '-') return false;

        // Month: chars 5..6
        if (!isDigit(s.charAt(5)) || !isDigit(s.charAt(6))) return false;
        if (s.charAt(7) != '-') return false;

        // Day: chars 8..9
        if (!isDigit(s.charAt(8)) || !isDigit(s.charAt(9))) return false;

        // Separator: char 10 (' ' or 'T')
        char sep = s.charAt(10);
        if (sep != ' ' && sep != 'T') return false;

        // Hour: chars 11..12
        if (!isDigit(s.charAt(11)) || !isDigit(s.charAt(12))) return false;
        if (s.charAt(13) != ':') return false;

        // Minute: chars 14..15
        if (!isDigit(s.charAt(14)) || !isDigit(s.charAt(15))) return false;
        if (s.charAt(16) != ':') return false;

        // Second: chars 17..18
        if (!isDigit(s.charAt(17)) || !isDigit(s.charAt(18))) return false;

        // Range checks
        int year = parse4Digits(s, 0);
        int month = parse2Digits(s, 5);
        int day = parse2Digits(s, 8);
        int hour = parse2Digits(s, 11);
        int min = parse2Digits(s, 14);
        int sec = parse2Digits(s, 17);

        if (month < 1 || month > 12) return false;

        int maxDays = DAYS_IN_MONTH[month];
        if (month == 2 && isLeapYear(year)) {
            maxDays = 29;
        }
        if (day < 1 || day > maxDays) return false;

        if (hour < 0 || hour > 23) return false;
        if (min < 0 || min > 59) return false;
        if (sec < 0 || sec > 59) return false;

        return true;
    }

    public static int parse4Digits(String s, int offset) {
        return (s.charAt(offset) - '0') * 1000
             + (s.charAt(offset + 1) - '0') * 100
             + (s.charAt(offset + 2) - '0') * 10
             + (s.charAt(offset + 3) - '0');
    }

    public static int parse2Digits(String s, int offset) {
        return (s.charAt(offset) - '0') * 10
             + (s.charAt(offset + 1) - '0');
    }

    /**
     * Extracts date string "yyyy-MM-dd" from valid timestamp.
     */
    public static String extractDate(String s) {
        if (s == null || s.length() < 10) return "unknown";
        return s.substring(0, 10);
    }

    /**
     * Extracts hour integer (0-23) from valid timestamp. Returns -1 if invalid.
     */
    public static int extractHour(String s) {
        if (s == null || s.length() < 13) return -1;
        return parse2Digits(s, 11);
    }

    /**
     * Computes UTC epoch seconds from a valid timestamp string.
     */
    public static long toEpochSeconds(String s) {
        int year = parse4Digits(s, 0);
        int month = parse2Digits(s, 5);
        int day = parse2Digits(s, 8);
        int hour = parse2Digits(s, 11);
        int min = parse2Digits(s, 14);
        int sec = parse2Digits(s, 17);

        LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, min, sec);
        return ldt.toEpochSecond(ZoneOffset.UTC);
    }

    /**
     * Converts UTC epoch seconds to ISO date string "yyyy-MM-dd".
     */
    public static String epochSecondsToDate(long epochSeconds) {
        if (epochSeconds <= 0L) {
            return "unknown";
        }
        return java.time.Instant.ofEpochSecond(epochSeconds)
                .atZone(ZoneOffset.UTC)
                .toLocalDate()
                .toString();
    }

    /**
     * Fast validation of date string "yyyy-MM-dd":
     * - Length == 10
     * - Chars 0..3: digits (year)
     * - Char 4: '-'
     * - Chars 5..6: digits, month in 1..12
     * - Char 7: '-'
     * - Chars 8..9: digits, day in 1..maxDays (respecting leap years)
     */
    public static boolean isValidDate(String s) {
        if (s == null || s.length() != 10) {
            return false;
        }
        for (int i = 0; i < 4; i++) {
            if (!isDigit(s.charAt(i))) return false;
        }
        if (s.charAt(4) != '-') return false;
        if (!isDigit(s.charAt(5)) || !isDigit(s.charAt(6))) return false;
        if (s.charAt(7) != '-') return false;
        if (!isDigit(s.charAt(8)) || !isDigit(s.charAt(9))) return false;

        int year = parse4Digits(s, 0);
        int month = parse2Digits(s, 5);
        int day = parse2Digits(s, 8);

        if (month < 1 || month > 12) {
            return false;
        }

        int maxDays = DAYS_IN_MONTH[month];
        if (month == 2 && isLeapYear(year)) {
            maxDays = 29;
        }
        return day >= 1 && day <= maxDays;
    }
}
