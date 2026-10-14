package edu.bigdata.clickstream.writable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class SessionAggWritableTest {

    @Test
    @DisplayName("Serialization round-trip preserves minEpoch, flagsMask, and removeCount")
    void testSerializationRoundTrip() throws Exception {
        SessionAggWritable original = new SessionAggWritable(
            1572566400L,
            (byte) (SessionAggWritable.FLAG_VIEW | SessionAggWritable.FLAG_CART),
            3
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);

        SessionAggWritable restored = new SessionAggWritable();
        restored.readFields(dis);

        assertEquals(original, restored);
        assertEquals(1572566400L, restored.getMinEpoch());
        assertEquals((byte) (SessionAggWritable.FLAG_VIEW | SessionAggWritable.FLAG_CART), restored.getFlagsMask());
        assertEquals(3, restored.getRemoveCount());
        assertTrue(restored.hasView());
        assertTrue(restored.hasCart());
        assertFalse(restored.hasPurchase());
        assertTrue(restored.isAbandoned());
    }

    @Test
    @DisplayName("Merge combines minEpoch (min), flagsMask (OR), and removeCount (sum)")
    void testMerge() {
        // Session event 1: View at epoch 100
        SessionAggWritable w1 = new SessionAggWritable(100L, SessionAggWritable.FLAG_VIEW, 0);

        // Session event 2: Cart at epoch 200, 1 removal
        SessionAggWritable w2 = new SessionAggWritable(200L, SessionAggWritable.FLAG_CART, 1);

        w1.merge(w2);

        assertEquals(100L, w1.getMinEpoch());
        assertEquals(1, w1.getRemoveCount());
        assertTrue(w1.hasView());
        assertTrue(w1.hasCart());
        assertFalse(w1.hasPurchase());
        assertTrue(w1.isAbandoned());

        // Session event 3: Purchase at epoch 300
        SessionAggWritable w3 = new SessionAggWritable(300L, SessionAggWritable.FLAG_PURCHASE, 0);
        w1.merge(w3);

        assertEquals(100L, w1.getMinEpoch());
        assertTrue(w1.hasView());
        assertTrue(w1.hasCart());
        assertTrue(w1.hasPurchase());
        assertFalse(w1.isAbandoned(), "Cart + Purchase session must not be abandoned");
    }

    @Test
    @DisplayName("Merge handles zero initial minEpoch properly")
    void testMergeZeroMinEpoch() {
        SessionAggWritable w1 = new SessionAggWritable(); // minEpoch = 0
        SessionAggWritable w2 = new SessionAggWritable(500L, SessionAggWritable.FLAG_VIEW, 1);

        w1.merge(w2);

        assertEquals(500L, w1.getMinEpoch());
        assertEquals(SessionAggWritable.FLAG_VIEW, w1.getFlagsMask());
        assertEquals(1, w1.getRemoveCount());
    }
}
