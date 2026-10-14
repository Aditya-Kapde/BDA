package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.writable.DailyFunnelWritable;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DailyFunnelCombinerTest {

    private DailyFunnelCombiner combiner;

    @BeforeEach
    void setUp() {
        combiner = new DailyFunnelCombiner();
    }

    @SuppressWarnings("unchecked")
    private DailyFunnelWritable runCombiner(String dateKey, List<DailyFunnelWritable> inputs) throws Exception {
        final List<DailyFunnelWritable> captured = new ArrayList<>();
        ReduceContext<Text, DailyFunnelWritable, Text, DailyFunnelWritable> reduceCtx =
            (ReduceContext<Text, DailyFunnelWritable, Text, DailyFunnelWritable>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        DailyFunnelWritable v = (DailyFunnelWritable) args[1];
                        captured.add(new DailyFunnelWritable(v.getSessions(), v.getCart(), v.getPurchase(), v.getAbandoned(), v.getRemoves()));
                    }
                    return null;
                }
            );

        Reducer<Text, DailyFunnelWritable, Text, DailyFunnelWritable>.Context combCtx =
            new WrappedReducer<Text, DailyFunnelWritable, Text, DailyFunnelWritable>().getReducerContext(reduceCtx);

        combiner.reduce(new Text(dateKey), inputs, combCtx);
        assertFalse(captured.isEmpty(), "Combiner should have emitted a combined record");
        return captured.get(0);
    }

    @Test
    @DisplayName("Combiner element-wise sums multiple partial inputs")
    void testBasicCombiner() throws Exception {
        List<DailyFunnelWritable> inputs = Arrays.asList(
            new DailyFunnelWritable(10L, 4L, 2L, 2L, 1L),
            new DailyFunnelWritable(20L, 8L, 5L, 3L, 2L),
            new DailyFunnelWritable(5L, 1L, 1L, 0L, 0L)
        );

        DailyFunnelWritable result = runCombiner("2019-11-01", inputs);

        assertEquals(35L, result.getSessions());
        assertEquals(13L, result.getCart());
        assertEquals(8L, result.getPurchase());
        assertEquals(5L, result.getAbandoned());
        assertEquals(3L, result.getRemoves());
    }

    @Test
    @DisplayName("Algebraic Proof: Associativity (A + (B + C) == (A + B) + C)")
    void testAssociativity() {
        DailyFunnelWritable a = new DailyFunnelWritable(10L, 4L, 2L, 2L, 1L);
        DailyFunnelWritable b = new DailyFunnelWritable(20L, 8L, 5L, 3L, 2L);
        DailyFunnelWritable c = new DailyFunnelWritable(30L, 12L, 7L, 5L, 4L);

        // (A + B) + C
        DailyFunnelWritable left = new DailyFunnelWritable(a.getSessions(), a.getCart(), a.getPurchase(), a.getAbandoned(), a.getRemoves());
        left.merge(b);
        left.merge(c);

        // A + (B + C)
        DailyFunnelWritable bc = new DailyFunnelWritable(b.getSessions(), b.getCart(), b.getPurchase(), b.getAbandoned(), b.getRemoves());
        bc.merge(c);
        DailyFunnelWritable right = new DailyFunnelWritable(a.getSessions(), a.getCart(), a.getPurchase(), a.getAbandoned(), a.getRemoves());
        right.merge(bc);

        assertEquals(left, right);
    }

    @Test
    @DisplayName("Algebraic Proof: Commutativity (A + B == B + A)")
    void testCommutativity() {
        DailyFunnelWritable a = new DailyFunnelWritable(100L, 50L, 30L, 20L, 10L);
        DailyFunnelWritable b = new DailyFunnelWritable(200L, 80L, 60L, 20L, 15L);

        DailyFunnelWritable ab = new DailyFunnelWritable(a.getSessions(), a.getCart(), a.getPurchase(), a.getAbandoned(), a.getRemoves());
        ab.merge(b);

        DailyFunnelWritable ba = new DailyFunnelWritable(b.getSessions(), b.getCart(), b.getPurchase(), b.getAbandoned(), b.getRemoves());
        ba.merge(a);

        assertEquals(ab, ba);
    }

    @Test
    @DisplayName("Combiner Equivalence: Direct merge vs 1 Combiner pass vs Multi-chunk Combiners")
    void testCombinerEquivalence() throws Exception {
        List<DailyFunnelWritable> inputs = Arrays.asList(
            new DailyFunnelWritable(10L, 3L, 2L, 1L, 0L),
            new DailyFunnelWritable(25L, 10L, 6L, 4L, 2L),
            new DailyFunnelWritable(15L, 5L, 3L, 2L, 1L),
            new DailyFunnelWritable(50L, 20L, 12L, 8L, 5L)
        );

        // 1. Direct merge
        DailyFunnelWritable direct = new DailyFunnelWritable();
        for (DailyFunnelWritable w : inputs) {
            direct.merge(w);
        }

        // 2. Single Combiner pass
        DailyFunnelWritable singlePass = runCombiner("2019-11-01", inputs);
        assertEquals(direct, singlePass);

        // 3. Multi-chunk passes (chunk size 2)
        DailyFunnelWritable chunk1 = runCombiner("2019-11-01", inputs.subList(0, 2));
        DailyFunnelWritable chunk2 = runCombiner("2019-11-01", inputs.subList(2, 4));
        DailyFunnelWritable multiPass = runCombiner("2019-11-01", Arrays.asList(chunk1, chunk2));

        assertEquals(direct, multiPass);
        assertEquals(100L, multiPass.getSessions());
        assertEquals(38L, multiPass.getCart());
        assertEquals(23L, multiPass.getPurchase());
        assertEquals(15L, multiPass.getAbandoned());
        assertEquals(8L, multiPass.getRemoves());
    }
}
