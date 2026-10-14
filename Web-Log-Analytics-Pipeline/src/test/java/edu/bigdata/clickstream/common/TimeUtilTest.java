package edu.bigdata.clickstream.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

public class TimeUtilTest {

    @Test
    @DisplayName("Valid timestamps must pass verification")
    void testValidTimestamps() {
        assertTrue(TimeUtil.isValidTimestamp("2019-11-01 00:00:02 UTC"));
        assertTrue(TimeUtil.isValidTimestamp("2019-11-01 23:59:59 UTC"));
        assertTrue(TimeUtil.isValidTimestamp("2019-10-31 12:30:45"));
        assertTrue(TimeUtil.isValidTimestamp("2020-02-29 00:00:00 UTC")); // Leap year
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "2019-13-01 00:00:00 UTC", // Month 13
        "2019-00-01 00:00:00 UTC", // Month 0
        "2019-02-29 00:00:00 UTC", // Feb 29 non-leap
        "2019-04-31 00:00:00 UTC", // April 31 (Apr has 30 days)
        "2019-11-01 24:00:00 UTC", // Hour 24
        "2019-11-01 12:60:00 UTC", // Minute 60
        "2019-11-01 12:30:60 UTC", // Second 60
        "short_timestamp",
        "2019/11/01 12:00:00",     // Wrong separator
        "2019-11-01x12:00:00",     // Wrong separator between date and time
        "2019-11-01 12.00.00"      // Wrong time separator
    })
    @DisplayName("Invalid timestamps must fail verification")
    void testInvalidTimestamps(String ts) {
        assertFalse(TimeUtil.isValidTimestamp(ts));
    }

    @Test
    @DisplayName("extractDate and extractHour must return correct UTC components")
    void testDateAndHourExtraction() {
        String ts = "2019-11-05 17:45:12 UTC";
        assertEquals("2019-11-05", TimeUtil.extractDate(ts));
        assertEquals(17, TimeUtil.extractHour(ts));
    }

    @Test
    @DisplayName("toEpochSeconds must compute correct UTC epoch seconds")
    void testEpochConversion() {
        String ts = "2019-11-01 00:00:00 UTC";
        long epoch = TimeUtil.toEpochSeconds(ts);
        assertEquals(1572566400L, epoch);

        String ts2 = "2019-11-01 00:00:02 UTC";
        assertEquals(1572566402L, TimeUtil.toEpochSeconds(ts2));
    }

    @Test
    @DisplayName("Leap year calculation check")
    void testLeapYear() {
        assertTrue(TimeUtil.isLeapYear(2020));
        assertTrue(TimeUtil.isLeapYear(2000));
        assertFalse(TimeUtil.isLeapYear(2019));
        assertFalse(TimeUtil.isLeapYear(1900));
        assertFalse(TimeUtil.isLeapYear(2100));
    }

    @Test
    @DisplayName("isValidDate validates ISO calendar dates and leap years correctly")
    void testIsValidDate() {
        assertTrue(TimeUtil.isValidDate("2019-11-01"));
        assertTrue(TimeUtil.isValidDate("2020-02-29")); // Leap year
        assertFalse(TimeUtil.isValidDate("2019-02-29")); // Non-leap year
        assertFalse(TimeUtil.isValidDate("2019-13-01")); // Bad month
        assertFalse(TimeUtil.isValidDate("2019-00-01"));
        assertFalse(TimeUtil.isValidDate("2019-04-31")); // Apr 31
        assertFalse(TimeUtil.isValidDate("2019-11-32"));
        assertFalse(TimeUtil.isValidDate("short"));
        assertFalse(TimeUtil.isValidDate(null));
        assertFalse(TimeUtil.isValidDate("2019/11/01"));
    }
}
