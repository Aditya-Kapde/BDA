package edu.bigdata.clickstream.writable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class ViewCountWritableTest {

    @Test
    @DisplayName("Serialization round-trip preserves count, category, and brand")
    void testSerializationRoundTrip() throws Exception {
        ViewCountWritable original = new ViewCountWritable(42L, "electronics.smartphone", "samsung");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);

        ViewCountWritable restored = new ViewCountWritable();
        restored.readFields(dis);

        assertEquals(original, restored);
        assertEquals(42L, restored.getCount());
        assertEquals("electronics.smartphone", restored.getCategory());
        assertEquals("samsung", restored.getBrand());
    }

    @Test
    @DisplayName("Merge accumulates count and picks lexicographically smallest non-empty metadata")
    void testMergeLexicographicalResolution() {
        ViewCountWritable w1 = new ViewCountWritable(10L, "electronics.audio", "sony");
        ViewCountWritable w2 = new ViewCountWritable(5L, "appliances.kitchen", "apple");

        w1.merge(w2);

        assertEquals(15L, w1.getCount());
        // "appliances.kitchen" < "electronics.audio"
        assertEquals("appliances.kitchen", w1.getCategory());
        // "apple" < "sony"
        assertEquals("apple", w1.getBrand());
    }

    @Test
    @DisplayName("Merge ignores empty strings when non-empty string is present")
    void testMergeWithEmptyStrings() {
        ViewCountWritable w1 = new ViewCountWritable(10L, "", "");
        ViewCountWritable w2 = new ViewCountWritable(5L, "electronics", "acme");

        w1.merge(w2);

        assertEquals(15L, w1.getCount());
        assertEquals("electronics", w1.getCategory());
        assertEquals("acme", w1.getBrand());

        // Merging another empty does not overwrite non-empty
        w1.merge(new ViewCountWritable(1L, "", ""));
        assertEquals(16L, w1.getCount());
        assertEquals("electronics", w1.getCategory());
        assertEquals("acme", w1.getBrand());
    }
}
