package edu.bigdata.clickstream.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class ClickEventParserTest {

    private ClickEventParser parser;
    private ClickEvent event;

    @BeforeEach
    void setUp() {
        parser = new ClickEventParser();
        event = new ClickEvent();
    }

    @Test
    @DisplayName("Rule 1: Header line must return HEADER status and HEADER_SKIPPED counter")
    void testHeaderDetection() {
        String header = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session";
        ClickEventParser.ParseResult res = parser.parse(header, event);

        assertEquals(ClickEventParser.ParseStatus.HEADER, res.getStatus());
        assertEquals(QualityCounter.HEADER_SKIPPED, res.getCounter());
        assertFalse(res.isAttributable());
    }

    @Test
    @DisplayName("Rule 2: Column count fewer than 9 must yield MALFORMED_COLUMN_COUNT")
    void testColumnCountTooFew() {
        String line = "2019-11-01 01:30:00 UTC,view,1001,2001";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_COLUMN_COUNT, res.getCounter());
        assertFalse(res.isAttributable());
    }

    @Test
    @DisplayName("Rule 2: Column count greater than 9 must yield MALFORMED_COLUMN_COUNT")
    void testColumnCountTooMany() {
        String line = "2019-11-01 01:35:00 UTC,view,1001,2001,x.y,acme,3.00,15,sess-F,extra_col";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_COLUMN_COUNT, res.getCounter());
        assertFalse(res.isAttributable());
    }

    @Test
    @DisplayName("Rule 2: Blank line must yield MALFORMED_COLUMN_COUNT")
    void testBlankLine() {
        ClickEventParser.ParseResult res1 = parser.parse("   ", event);
        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res1.getStatus());
        assertEquals(QualityCounter.MALFORMED_COLUMN_COUNT, res1.getCounter());

        ClickEventParser.ParseResult res2 = parser.parse("", event);
        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res2.getStatus());
        assertEquals(QualityCounter.MALFORMED_COLUMN_COUNT, res2.getCounter());
    }

    @Test
    @DisplayName("Rule 2: Quoted field containing comma must be parsed as a single column")
    void testQuotedFieldWithComma() {
        String line = "2019-11-01 00:00:02 UTC,view,1001,2001,\"electronics,audio.headphone\",acme,199.90,11,sess-A";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.VALID, res.getStatus());
        assertEquals(QualityCounter.VALID_RECORDS, res.getCounter());
        assertEquals("electronics,audio.headphone", event.getCategoryCode());
    }

    @Test
    @DisplayName("Rule 3: Invalid timestamp format or month must yield MALFORMED_TIMESTAMP (unattributable)")
    void testInvalidTimestamp() {
        // Month 13
        String line = "2019-13-01 01:10:00 UTC,view,1001,2001,electronics.smartphone,acme,199.90,14,sess-D";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_TIMESTAMP, res.getCounter());
        assertFalse(res.isAttributable());
        assertEquals("INVALID_TIME", res.getDateHour());
    }

    @Test
    @DisplayName("Rule 3: Non-leap-year Feb 29 must yield MALFORMED_TIMESTAMP")
    void testNonLeapYearFeb29() {
        String line = "2019-02-29 01:00:00 UTC,view,1001,2001,electronics.smartphone,acme,199.90,14,sess-D";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_TIMESTAMP, res.getCounter());
    }

    @Test
    @DisplayName("Rule 4: Invalid event type must yield MALFORMED_EVENT_TYPE (attributable)")
    void testInvalidEventType() {
        String line = "2019-11-01 01:40:00 UTC,checkout,1001,2001,x.y,acme,3.00,15,sess-G";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_EVENT_TYPE, res.getCounter());
        assertTrue(res.isAttributable());
        assertEquals("2019-11-01 01", res.getDateHour());
    }

    @Test
    @DisplayName("Rule 5: Non-numeric ID must yield MALFORMED_ID (attributable)")
    void testInvalidId() {
        String line = "2019-11-01 01:20:00 UTC,view,abc,2001,x.y,acme,3.00,15,sess-E";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_ID, res.getCounter());
        assertTrue(res.isAttributable());
        assertEquals("2019-11-01 01", res.getDateHour());
    }

    @Test
    @DisplayName("Rule 6: Negative price must yield MALFORMED_PRICE (attributable)")
    void testNegativePrice() {
        String line = "2019-11-01 01:45:00 UTC,view,1001,2001,x.y,acme,-15.00,15,sess-H";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_PRICE, res.getCounter());
        assertTrue(res.isAttributable());
    }

    @Test
    @DisplayName("Rule 7: Empty session must yield MALFORMED_SESSION (attributable)")
    void testEmptySession() {
        String line = "2019-11-01 01:50:00 UTC,view,1001,2001,x.y,acme,15.00,15,";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.MALFORMED, res.getStatus());
        assertEquals(QualityCounter.MALFORMED_SESSION, res.getCounter());
        assertTrue(res.isAttributable());
    }

    @Test
    @DisplayName("Missing category and brand must remain VALID records with informational flags")
    void testMissingCategoryAndBrand() {
        String line = "2019-11-01 00:59:59 UTC,view,1002,2002,,,5.00,12,sess-B";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.VALID, res.getStatus());
        assertEquals(QualityCounter.VALID_RECORDS, res.getCounter());
        assertTrue(res.isMissingCategory());
        assertTrue(res.isMissingBrand());
        assertEquals("", event.getCategoryCode());
        assertEquals("", event.getBrand());
    }

    @ParameterizedTest
    @ValueSource(strings = {"view", "cart", "remove_from_cart", "purchase"})
    @DisplayName("All four event types must be recognized as valid")
    void testAllEventTypes(String typeStr) {
        String line = "2019-11-01 10:00:00 UTC," + typeStr + ",1001,2001,cat,brand,10.00,1,sess-1";
        ClickEventParser.ParseResult res = parser.parse(line, event);

        assertEquals(ClickEventParser.ParseStatus.VALID, res.getStatus());
        assertEquals(QualityCounter.VALID_RECORDS, res.getCounter());
        assertNotNull(event.getEventType());
        assertEquals(typeStr, event.getEventType().getCsvValue());
    }

    @Test
    @DisplayName("Rule Precedence: First failing rule must win when multiple errors occur")
    void testRulePrecedence() {
        // Wrong column count (rule 2) AND invalid timestamp (rule 3) -> rule 2 wins
        String line1 = "2019-99-99 99:99:99 UTC,view,1001";
        ClickEventParser.ParseResult res1 = parser.parse(line1, event);
        assertEquals(QualityCounter.MALFORMED_COLUMN_COUNT, res1.getCounter());

        // Invalid timestamp (rule 3) AND invalid ID (rule 5) -> rule 3 wins
        String line2 = "2019-13-01 01:00:00 UTC,view,not_id,2001,cat,brand,10.00,1,sess-1";
        ClickEventParser.ParseResult res2 = parser.parse(line2, event);
        assertEquals(QualityCounter.MALFORMED_TIMESTAMP, res2.getCounter());

        // Invalid event type (rule 4) AND negative price (rule 6) -> rule 4 wins
        String line3 = "2019-11-01 01:00:00 UTC,invalid_type,1001,2001,cat,brand,-5.00,1,sess-1";
        ClickEventParser.ParseResult res3 = parser.parse(line3, event);
        assertEquals(QualityCounter.MALFORMED_EVENT_TYPE, res3.getCounter());

        // Invalid ID (rule 5) AND empty session (rule 7) -> rule 5 wins
        String line4 = "2019-11-01 01:00:00 UTC,view,bad_id,2001,cat,brand,5.00,1,";
        ClickEventParser.ParseResult res4 = parser.parse(line4, event);
        assertEquals(QualityCounter.MALFORMED_ID, res4.getCounter());
    }

    @Test
    @DisplayName("Comprehensive fixture verification on mini_clickstream.csv")
    void testMiniClickstreamFixture() throws Exception {
        InputStream is = getClass().getResourceAsStream("/fixtures/mini_clickstream.csv");
        assertNotNull(is, "mini_clickstream.csv fixture must exist");

        int validCount = 0;
        int headerCount = 0;
        int malformedCount = 0;
        int missingCategoryCount = 0;
        int missingBrandCount = 0;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                ClickEventParser.ParseResult res = parser.parse(line, event);

                if (res.getStatus() == ClickEventParser.ParseStatus.HEADER) {
                    headerCount++;
                    assertEquals(1, lineNum, "Header must be on line 1");
                } else if (res.getStatus() == ClickEventParser.ParseStatus.VALID) {
                    validCount++;
                    if (res.isMissingCategory()) missingCategoryCount++;
                    if (res.isMissingBrand()) missingBrandCount++;
                } else {
                    malformedCount++;
                }
            }
        }

        assertEquals(1, headerCount, "Expected exactly 1 header");
        assertEquals(10, validCount, "Expected 10 valid records in mini_clickstream");
        assertEquals(8, malformedCount, "Expected 8 malformed records in mini_clickstream");
        assertTrue(missingCategoryCount >= 1, "Expected missing category records");
        assertTrue(missingBrandCount >= 1, "Expected missing brand records");
    }
}
