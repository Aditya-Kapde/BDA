package edu.bigdata.clickstream.writable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class EventCountWritableTest {

    @Test
    @DisplayName("Serialization round-trip preserves all five fields")
    void testSerializationRoundTrip() throws Exception {
        EventCountWritable original = new EventCountWritable(100L, 25L, 5L, 10L, 2L);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);

        EventCountWritable restored = new EventCountWritable();
        restored.readFields(dis);

        assertEquals(original, restored);
        assertEquals(100L, restored.getViews());
        assertEquals(25L, restored.getCarts());
        assertEquals(5L, restored.getRemoves());
        assertEquals(10L, restored.getPurchases());
        assertEquals(2L, restored.getMalformed());
    }

    @Test
    @DisplayName("Merge method correctly sums fields associatively and commutatively")
    void testMerge() {
        EventCountWritable w1 = new EventCountWritable(10L, 5L, 2L, 1L, 0L);
        EventCountWritable w2 = new EventCountWritable(20L, 10L, 3L, 2L, 1L);

        w1.merge(w2);

        assertEquals(30L, w1.getViews());
        assertEquals(15L, w1.getCarts());
        assertEquals(5L, w1.getRemoves());
        assertEquals(3L, w1.getPurchases());
        assertEquals(1L, w1.getMalformed());
        assertEquals(53L, w1.getTotal());
    }

    @Test
    @DisplayName("Error rate calculation handles non-zero and zero cases safely")
    void testErrorRate() {
        EventCountWritable w = new EventCountWritable(80L, 10L, 5L, 5L, 20L);
        // Total = 100, malformed = 20 -> 20 / 120 = 0.166666...
        assertEquals(20.0 / 120.0, w.getErrorRate(), 1e-6);

        EventCountWritable zero = new EventCountWritable(0L, 0L, 0L, 0L, 0L);
        assertEquals(0.0, zero.getErrorRate(), 1e-9);
    }

    @Test
    @DisplayName("Repeated deserialization does not leave stale state")
    void testRepeatedDeserialization() throws Exception {
        EventCountWritable w = new EventCountWritable(999L, 888L, 777L, 666L, 555L);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        new EventCountWritable(1L, 2L, 3L, 4L, 5L).write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        w.readFields(dis);

        assertEquals(1L, w.getViews());
        assertEquals(2L, w.getCarts());
        assertEquals(3L, w.getRemoves());
        assertEquals(4L, w.getPurchases());
        assertEquals(5L, w.getMalformed());
    }
}
