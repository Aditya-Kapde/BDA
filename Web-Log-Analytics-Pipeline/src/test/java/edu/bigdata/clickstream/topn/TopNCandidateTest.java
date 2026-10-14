package edu.bigdata.clickstream.topn;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TopNCandidateTest {

    @Test
    @DisplayName("Parse valid product record with category and brand")
    void testParseValidProduct() {
        String line = "1001\telectronics.smartphone\tacme\t350";
        TopNCandidate c = TopNCandidate.parse(line);
        assertNotNull(c);
        assertEquals(350, c.getViews());
        assertEquals("1001", c.getKey());
        assertEquals(line, c.getOriginalLine());
    }

    @Test
    @DisplayName("Parse valid category record")
    void testParseValidCategory() {
        String line = "electronics.smartphone\t1500";
        TopNCandidate c = TopNCandidate.parse(line);
        assertNotNull(c);
        assertEquals(1500, c.getViews());
        assertEquals("electronics.smartphone", c.getKey());
        assertEquals(line, c.getOriginalLine());
    }

    @Test
    @DisplayName("Parse product record with empty category and empty brand")
    void testParseProductWithEmptyMetadata() {
        String line = "1002\t\t\t15";
        TopNCandidate c = TopNCandidate.parse(line);
        assertNotNull(c);
        assertEquals(15, c.getViews());
        assertEquals("1002", c.getKey());
        assertEquals(line, c.getOriginalLine());
    }

    @Test
    @DisplayName("Parse rejects null, empty, comment, or malformed lines")
    void testParseInvalidLines() {
        assertNull(TopNCandidate.parse(null));
        assertNull(TopNCandidate.parse(""));
        assertNull(TopNCandidate.parse("   "));
        assertNull(TopNCandidate.parse("# comment"));
        assertNull(TopNCandidate.parse("single_column"));
        assertNull(TopNCandidate.parse("1001\tnot_a_number"));
        assertNull(TopNCandidate.parse("1001\t-10"));
        assertNull(TopNCandidate.parse("\t100")); // empty key
    }

    @Test
    @DisplayName("compareKeys performs numeric comparison for digit keys and lexical comparison for text keys")
    void testCompareKeys() {
        // Numeric keys: 20 < 100
        assertTrue(TopNCandidate.compareKeys("20", "100") < 0);
        assertTrue(TopNCandidate.compareKeys("100", "20") > 0);
        assertEquals(0, TopNCandidate.compareKeys("100", "100"));

        // Text keys: appliances.kitchen < electronics.smartphone
        assertTrue(TopNCandidate.compareKeys("appliances.kitchen", "electronics.smartphone") < 0);
        assertTrue(TopNCandidate.compareKeys("electronics.smartphone", "appliances.kitchen") > 0);

        // Mixed keys
        assertTrue(TopNCandidate.compareKeys("100", "appliances") < 0);
    }

    @Test
    @DisplayName("compareTo: higher views rank before lower views (descending)")
    void testCompareToViewsDescending() {
        TopNCandidate high = new TopNCandidate(100, "1001", "1001\t100");
        TopNCandidate low = new TopNCandidate(50, "1001", "1001\t50");

        // High should come before Low (negative compareTo)
        assertTrue(high.compareTo(low) < 0);
        assertTrue(low.compareTo(high) > 0);
    }

    @Test
    @DisplayName("compareTo: equal views tie-broken by key ascending")
    void testCompareToKeyAscendingTieBreak() {
        TopNCandidate c1 = new TopNCandidate(100, "1002", "1002\t100");
        TopNCandidate c2 = new TopNCandidate(100, "1004", "1004\t100");

        // c1 (1002) should come before c2 (1004)
        assertTrue(c1.compareTo(c2) < 0);
        assertTrue(c2.compareTo(c1) > 0);
    }

    @Test
    @DisplayName("compareTo: identical views and key distinguished by originalLine and seqId")
    void testCompareToDistinctInstancesPreserved() {
        TopNCandidate c1 = new TopNCandidate(100, "1001", "1001\tcatA\t100", 1L);
        TopNCandidate c2 = new TopNCandidate(100, "1001", "1001\tcatB\t100", 2L);
        assertTrue(c1.compareTo(c2) < 0);

        // Identical lines with different seqId
        TopNCandidate c3 = new TopNCandidate(100, "1001", "1001\tcatA\t100", 3L);
        TopNCandidate c4 = new TopNCandidate(100, "1001", "1001\tcatA\t100", 4L);
        assertTrue(c3.compareTo(c4) < 0);
        assertNotEquals(0, c3.compareTo(c4));

        // Self comparison returns 0
        assertEquals(0, c3.compareTo(c3));
    }

    @Test
    @DisplayName("Domain methods: equals, hashCode, toString")
    void testDomainMethods() {
        TopNCandidate c1 = new TopNCandidate(50, "k1", "k1\t50", 10L);
        TopNCandidate c2 = new TopNCandidate(50, "k1", "k1\t50", 10L);
        TopNCandidate c3 = new TopNCandidate(50, "k1", "k1\t50", 11L);

        assertEquals(c1, c2);
        assertNotEquals(c1, c3);
        assertNotEquals(c1, null);
        assertNotEquals(c1, "other");
        assertEquals(c1.hashCode(), c2.hashCode());
        assertTrue(c1.toString().contains("views=50"));
        assertTrue(c1.toString().contains("key='k1'"));
    }
}
