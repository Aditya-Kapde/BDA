package edu.bigdata.clickstream.common;

import edu.bigdata.clickstream.writable.EventCountWritable;
import edu.bigdata.clickstream.writable.SessionAggWritable;
import edu.bigdata.clickstream.writable.ViewCountWritable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DomainModelCoverageTest {

    @Test
    @DisplayName("ClickEvent accessors, equals, hashCode, and reset")
    void testClickEventAccessors() {
        ClickEvent event1 = new ClickEvent();
        event1.setEventTimeRaw("2019-11-01 00:00:02 UTC");
        event1.setEpochSeconds(1572566402L);
        event1.setLogDate("2019-11-01");
        event1.setHour(0);
        event1.setEventType(EventType.VIEW);
        event1.setProductId(1001L);
        event1.setCategoryId(2001L);
        event1.setCategoryCode("electronics.smartphone");
        event1.setBrand("acme");
        event1.setPrice(199.90);
        event1.setUserId(11L);
        event1.setUserSession("sess-A");

        assertEquals("2019-11-01 00:00:02 UTC", event1.getEventTimeRaw());
        assertEquals(1572566402L, event1.getEpochSeconds());
        assertEquals("2019-11-01", event1.getLogDate());
        assertEquals(0, event1.getHour());
        assertEquals(EventType.VIEW, event1.getEventType());
        assertEquals(1001L, event1.getProductId());
        assertEquals(2001L, event1.getCategoryId());
        assertEquals("electronics.smartphone", event1.getCategoryCode());
        assertEquals("acme", event1.getBrand());
        assertEquals(199.90, event1.getPrice());
        assertEquals(11L, event1.getUserId());
        assertEquals("sess-A", event1.getUserSession());

        ClickEvent event2 = new ClickEvent();
        event2.setEventTimeRaw("2019-11-01 00:00:02 UTC");
        event2.setEpochSeconds(1572566402L);
        event2.setLogDate("2019-11-01");
        event2.setHour(0);
        event2.setEventType(EventType.VIEW);
        event2.setProductId(1001L);
        event2.setCategoryId(2001L);
        event2.setCategoryCode("electronics.smartphone");
        event2.setBrand("acme");
        event2.setPrice(199.90);
        event2.setUserId(11L);
        event2.setUserSession("sess-A");

        assertEquals(event1, event2);
        assertEquals(event1.hashCode(), event2.hashCode());
        assertTrue(event1.toString().contains("product=1001"));

        event1.reset();
        assertEquals("", event1.getEventTimeRaw());
        assertEquals(0L, event1.getProductId());
        assertNotEquals(event1, event2);
        assertNotEquals(event1, null);
        assertNotEquals(event1, "other_type");
    }

    @Test
    @DisplayName("EventType values, flag bits, and unknown string rejection")
    void testEventTypeValues() {
        assertEquals("view", EventType.VIEW.getCsvValue());
        assertEquals(0x01, EventType.VIEW.getFlagBit());
        assertEquals(0x02, EventType.CART.getFlagBit());
        assertEquals(0x04, EventType.PURCHASE.getFlagBit());
        assertEquals(0x00, EventType.REMOVE_FROM_CART.getFlagBit());

        assertNull(EventType.fromCsvValue(null));
        assertNull(EventType.fromCsvValue("checkout"));
        assertNull(EventType.fromCsvValue(""));
        assertEquals("cart", EventType.CART.toString());
    }

    @Test
    @DisplayName("QualityCounter enum and group name")
    void testQualityCounterEnum() {
        assertEquals("ClickstreamQuality", QualityCounter.GROUP_NAME);
        assertEquals(QualityCounter.TOTAL_RECORDS, QualityCounter.valueOf("TOTAL_RECORDS"));
    }

    @Test
    @DisplayName("EventCountWritable accessors, equals, hashCode, and toString")
    void testEventCountWritableAccessors() {
        EventCountWritable w1 = new EventCountWritable(1, 2, 3, 4, 5);
        EventCountWritable w2 = new EventCountWritable(1, 2, 3, 4, 5);

        assertEquals(w1, w2);
        assertEquals(w1.hashCode(), w2.hashCode());
        assertEquals("1\t2\t3\t4\t5", w1.toString());

        w1.reset();
        assertEquals(0L, w1.getViews());
        assertNotEquals(w1, w2);
        assertNotEquals(w1, null);
    }

    @Test
    @DisplayName("ViewCountWritable accessors, equals, hashCode, and toString")
    void testViewCountWritableAccessors() {
        ViewCountWritable w1 = new ViewCountWritable(10, "cat", "brand");
        ViewCountWritable w2 = new ViewCountWritable(10, "cat", "brand");

        assertEquals(w1, w2);
        assertEquals(w1.hashCode(), w2.hashCode());
        assertEquals("10\tcat\tbrand", w1.toString());

        w1.reset();
        assertEquals(0L, w1.getCount());
        assertEquals("", w1.getCategory());
        assertEquals("", w1.getBrand());
    }

    @Test
    @DisplayName("SessionAggWritable accessors, equals, hashCode, and toString")
    void testSessionAggWritableAccessors() {
        SessionAggWritable w1 = new SessionAggWritable(100L, (byte) 7, 2);
        SessionAggWritable w2 = new SessionAggWritable(100L, (byte) 7, 2);

        assertEquals(w1, w2);
        assertEquals(w1.hashCode(), w2.hashCode());
        assertEquals("100\t7\t2", w1.toString());

        w1.reset();
        assertEquals(0L, w1.getMinEpoch());
        assertEquals(0, w1.getFlagsMask());
        assertEquals(0, w1.getRemoveCount());
    }

    @Test
    @DisplayName("DailyFunnelWritable accessors, equals, hashCode, and toString")
    void testDailyFunnelWritableAccessors() {
        edu.bigdata.clickstream.writable.DailyFunnelWritable w1 =
            new edu.bigdata.clickstream.writable.DailyFunnelWritable(60L, 20L, 12L, 8L, 4L);
        edu.bigdata.clickstream.writable.DailyFunnelWritable w2 =
            new edu.bigdata.clickstream.writable.DailyFunnelWritable(60L, 20L, 12L, 8L, 4L);

        assertEquals(w1, w2);
        assertEquals(w1.hashCode(), w2.hashCode());
        assertEquals("60\t20\t12\t8\t4", w1.toString());

        w1.reset();
        assertEquals(0L, w1.getSessions());
        assertEquals(0L, w1.getCart());
        assertEquals(0L, w1.getPurchase());
        assertEquals(0L, w1.getAbandoned());
        assertEquals(0L, w1.getRemoves());

        w1.setSessions(10L);
        w1.setCart(5L);
        w1.setPurchase(3L);
        w1.setAbandoned(2L);
        w1.setRemoves(1L);

        assertEquals(10L, w1.getSessions());
        assertEquals(5L, w1.getCart());
        assertEquals(3L, w1.getPurchase());
        assertEquals(2L, w1.getAbandoned());
        assertEquals(1L, w1.getRemoves());
        assertNotEquals(w1, null);
        assertNotEquals(w1, "other");
    }
}
