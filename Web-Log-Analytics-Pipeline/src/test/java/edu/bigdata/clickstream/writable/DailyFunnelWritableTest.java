package edu.bigdata.clickstream.writable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class DailyFunnelWritableTest {

    @Test
    @DisplayName("Serialization round-trip preserves all 5 metric fields")
    void testSerializationRoundTrip() throws Exception {
        DailyFunnelWritable original = new DailyFunnelWritable(100L, 40L, 25L, 15L, 8L);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);

        DailyFunnelWritable restored = new DailyFunnelWritable();
        restored.readFields(dis);

        assertEquals(original, restored);
        assertEquals(100L, restored.getSessions());
        assertEquals(40L, restored.getCart());
        assertEquals(25L, restored.getPurchase());
        assertEquals(15L, restored.getAbandoned());
        assertEquals(8L, restored.getRemoves());
    }

    @Test
    @DisplayName("Merge performs exact element-wise addition")
    void testMerge() {
        DailyFunnelWritable w1 = new DailyFunnelWritable(10L, 4L, 2L, 2L, 1L);
        DailyFunnelWritable w2 = new DailyFunnelWritable(15L, 6L, 5L, 1L, 3L);

        w1.merge(w2);

        assertEquals(25L, w1.getSessions());
        assertEquals(10L, w1.getCart());
        assertEquals(7L, w1.getPurchase());
        assertEquals(3L, w1.getAbandoned());
        assertEquals(4L, w1.getRemoves());
    }

    @Test
    @DisplayName("Reset restores all fields to 0")
    void testReset() {
        DailyFunnelWritable w = new DailyFunnelWritable(50L, 20L, 10L, 10L, 5L);
        w.reset();

        assertEquals(0L, w.getSessions());
        assertEquals(0L, w.getCart());
        assertEquals(0L, w.getPurchase());
        assertEquals(0L, w.getAbandoned());
        assertEquals(0L, w.getRemoves());
    }

    @Test
    @DisplayName("Associative and commutative algebraic properties")
    void testAlgebraicLaws() {
        DailyFunnelWritable a = new DailyFunnelWritable(10L, 5L, 3L, 2L, 1L);
        DailyFunnelWritable b = new DailyFunnelWritable(20L, 8L, 4L, 4L, 2L);
        DailyFunnelWritable c = new DailyFunnelWritable(30L, 12L, 7L, 5L, 4L);

        // (A + B) + C == A + (B + C)
        DailyFunnelWritable left = new DailyFunnelWritable(a.getSessions(), a.getCart(), a.getPurchase(), a.getAbandoned(), a.getRemoves());
        left.merge(b);
        left.merge(c);

        DailyFunnelWritable bc = new DailyFunnelWritable(b.getSessions(), b.getCart(), b.getPurchase(), b.getAbandoned(), b.getRemoves());
        bc.merge(c);
        DailyFunnelWritable right = new DailyFunnelWritable(a.getSessions(), a.getCart(), a.getPurchase(), a.getAbandoned(), a.getRemoves());
        right.merge(bc);

        assertEquals(left, right);

        // A + B == B + A
        DailyFunnelWritable ab = new DailyFunnelWritable(a.getSessions(), a.getCart(), a.getPurchase(), a.getAbandoned(), a.getRemoves());
        ab.merge(b);

        DailyFunnelWritable ba = new DailyFunnelWritable(b.getSessions(), b.getCart(), b.getPurchase(), b.getAbandoned(), b.getRemoves());
        ba.merge(a);

        assertEquals(ab, ba);
    }
}
