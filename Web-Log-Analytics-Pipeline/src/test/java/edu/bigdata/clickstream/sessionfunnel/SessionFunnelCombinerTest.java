package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.writable.SessionAggWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SessionFunnelCombinerTest {

    private SessionFunnelCombiner combiner;

    @BeforeEach
    void setUp() {
        combiner = new SessionFunnelCombiner();
    }

    @SuppressWarnings("unchecked")
    private SessionAggWritable runCombiner(String sessionKey, List<SessionAggWritable> inputs) throws Exception {
        final List<SessionAggWritable> captured = new ArrayList<>();
        ReduceContext<Text, SessionAggWritable, Text, SessionAggWritable> reduceCtx =
            (ReduceContext<Text, SessionAggWritable, Text, SessionAggWritable>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        SessionAggWritable v = (SessionAggWritable) args[1];
                        captured.add(new SessionAggWritable(v.getMinEpoch(), v.getFlagsMask(), v.getRemoveCount()));
                    }
                    return null;
                }
            );

        Reducer<Text, SessionAggWritable, Text, SessionAggWritable>.Context combCtx =
            new WrappedReducer<Text, SessionAggWritable, Text, SessionAggWritable>().getReducerContext(reduceCtx);

        combiner.reduce(new Text(sessionKey), inputs, combCtx);
        assertFalse(captured.isEmpty(), "Combiner should have emitted a combined record");
        return captured.get(0);
    }

    @Test
    @DisplayName("Combiner algebraically combines minEpoch, flagsMask, and removeCount")
    void testBasicCombiner() throws Exception {
        List<SessionAggWritable> inputs = Arrays.asList(
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572566410L, SessionAggWritable.FLAG_CART, 1),
            new SessionAggWritable(1572566420L, SessionAggWritable.FLAG_PURCHASE, 0)
        );

        SessionAggWritable result = runCombiner("sess-1", inputs);

        assertEquals(1572566400L, result.getMinEpoch());
        assertTrue(result.hasView());
        assertTrue(result.hasCart());
        assertTrue(result.hasPurchase());
        assertFalse(result.isAbandoned(), "Cart + purchase cannot be abandoned");
        assertEquals(1, result.getRemoveCount());
    }

    @Test
    @DisplayName("Algebraic Proof: Associativity (A + (B + C) == (A + B) + C)")
    void testAssociativity() {
        SessionAggWritable a = new SessionAggWritable(100L, SessionAggWritable.FLAG_VIEW, 0);
        SessionAggWritable b = new SessionAggWritable(200L, SessionAggWritable.FLAG_CART, 2);
        SessionAggWritable c = new SessionAggWritable(150L, SessionAggWritable.FLAG_PURCHASE, 1);

        // (A + B) + C
        SessionAggWritable left = new SessionAggWritable(a.getMinEpoch(), a.getFlagsMask(), a.getRemoveCount());
        left.merge(b);
        left.merge(c);

        // A + (B + C)
        SessionAggWritable bc = new SessionAggWritable(b.getMinEpoch(), b.getFlagsMask(), b.getRemoveCount());
        bc.merge(c);
        SessionAggWritable right = new SessionAggWritable(a.getMinEpoch(), a.getFlagsMask(), a.getRemoveCount());
        right.merge(bc);

        assertEquals(left.getMinEpoch(), right.getMinEpoch());
        assertEquals(left.getFlagsMask(), right.getFlagsMask());
        assertEquals(left.getRemoveCount(), right.getRemoveCount());
        assertEquals(left, right);
    }

    @Test
    @DisplayName("Algebraic Proof: Commutativity (A + B == B + A)")
    void testCommutativity() {
        SessionAggWritable a = new SessionAggWritable(100L, SessionAggWritable.FLAG_VIEW, 1);
        SessionAggWritable b = new SessionAggWritable(50L, SessionAggWritable.FLAG_CART, 3);

        SessionAggWritable ab = new SessionAggWritable(a.getMinEpoch(), a.getFlagsMask(), a.getRemoveCount());
        ab.merge(b);

        SessionAggWritable ba = new SessionAggWritable(b.getMinEpoch(), b.getFlagsMask(), b.getRemoveCount());
        ba.merge(a);

        assertEquals(ab.getMinEpoch(), ba.getMinEpoch());
        assertEquals(ab.getFlagsMask(), ba.getFlagsMask());
        assertEquals(ab.getRemoveCount(), ba.getRemoveCount());
        assertEquals(ab, ba);
    }

    @Test
    @DisplayName("Combiner Equivalence: Direct merge vs 1 Combiner pass vs Multi-chunk Combiners")
    void testCombinerEquivalence() throws Exception {
        List<SessionAggWritable> events = Arrays.asList(
            new SessionAggWritable(300L, SessionAggWritable.FLAG_PURCHASE, 0),
            new SessionAggWritable(100L, SessionAggWritable.FLAG_VIEW, 1),
            new SessionAggWritable(200L, SessionAggWritable.FLAG_CART, 2),
            new SessionAggWritable(150L, (byte) 0, 1) // removal only
        );

        // 1. Direct merge
        SessionAggWritable direct = new SessionAggWritable();
        for (SessionAggWritable ev : events) {
            direct.merge(ev);
        }

        // 2. Single Combiner pass
        SessionAggWritable singlePass = runCombiner("sess-eq", events);
        assertEquals(direct, singlePass);

        // 3. Multi-chunk Combiner pass (chunks of 2)
        SessionAggWritable chunk1 = runCombiner("sess-eq", events.subList(0, 2));
        SessionAggWritable chunk2 = runCombiner("sess-eq", events.subList(2, 4));
        SessionAggWritable combinedChunks = runCombiner("sess-eq", Arrays.asList(chunk1, chunk2));

        assertEquals(direct, combinedChunks);
        assertEquals(100L, combinedChunks.getMinEpoch());
        assertTrue(combinedChunks.hasView());
        assertTrue(combinedChunks.hasCart());
        assertTrue(combinedChunks.hasPurchase());
        assertEquals(4, combinedChunks.getRemoveCount());
    }

    @Test
    @DisplayName("Combiner preserves merged zero minEpoch without throwing error")
    void testZeroMinEpochPreserved() throws Exception {
        List<SessionAggWritable> inputs = Collections.singletonList(
            new SessionAggWritable(0L, (byte) 0, 0)
        );

        final List<SessionAggWritable> captured = new ArrayList<>();
        @SuppressWarnings("unchecked")
        ReduceContext<Text, SessionAggWritable, Text, SessionAggWritable> reduceCtx =
            (ReduceContext<Text, SessionAggWritable, Text, SessionAggWritable>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        captured.add((SessionAggWritable) args[1]);
                    }
                    return null;
                }
            );

        Reducer<Text, SessionAggWritable, Text, SessionAggWritable>.Context combCtx =
            new WrappedReducer<Text, SessionAggWritable, Text, SessionAggWritable>().getReducerContext(reduceCtx);

        combiner.reduce(new Text("sess-zero"), inputs, combCtx);
        assertEquals(1, captured.size(), "Combiner should emit merged accumulator");
        assertEquals(0L, captured.get(0).getMinEpoch());
    }
}
