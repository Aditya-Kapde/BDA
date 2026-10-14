package edu.bigdata.clickstream.traffic;

import edu.bigdata.clickstream.writable.EventCountWritable;
import org.apache.hadoop.io.NullWritable;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * CombinerEquivalenceTest proves algebraically and functionally:
 * 1. Direct Reducer aggregation == 1 Combiner pass + Reducer == Multi-Chunk Combiners + Reducer
 * 2. EventCountWritable associative and commutative properties hold under arbitrary chunking
 * 3. Both regular attributable events and unattributable INVALID_TIME records yield identical results.
 */
public class CombinerEquivalenceTest {

    private HourlyTrafficCombiner combiner;
    private HourlyTrafficReducer reducer;

    @BeforeEach
    void setUp() {
        combiner = new HourlyTrafficCombiner();
        reducer = new HourlyTrafficReducer();
    }

    /**
     * Helper to execute Reducer directly on a raw list of EventCountWritable values.
     */
    @SuppressWarnings("unchecked")
    private String runDirectReducer(String keyStr, List<EventCountWritable> rawValues) throws Exception {
        final List<String> captured = new ArrayList<>();
        ReduceContext<Text, EventCountWritable, NullWritable, Text> reduceCtx =
            (ReduceContext<Text, EventCountWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        captured.add(args[1].toString());
                        return null;
                    }
                    return null;
                }
            );
        Reducer<Text, EventCountWritable, NullWritable, Text>.Context redCtx =
            new WrappedReducer<Text, EventCountWritable, NullWritable, Text>().getReducerContext(reduceCtx);

        reducer.reduce(new Text(keyStr), rawValues, redCtx);
        return captured.get(0);
    }

    /**
     * Helper to execute Combiner on a batch of EventCountWritable values and return combined snapshot.
     */
    @SuppressWarnings("unchecked")
    private EventCountWritable runCombiner(String keyStr, List<EventCountWritable> values) throws Exception {
        final List<EventCountWritable> captured = new ArrayList<>();
        ReduceContext<Text, EventCountWritable, Text, EventCountWritable> reduceCtx =
            (ReduceContext<Text, EventCountWritable, Text, EventCountWritable>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        EventCountWritable v = (EventCountWritable) args[1];
                        captured.add(new EventCountWritable(v.getViews(), v.getCarts(), v.getRemoves(), v.getPurchases(), v.getMalformed()));
                        return null;
                    }
                    return null;
                }
            );
        Reducer<Text, EventCountWritable, Text, EventCountWritable>.Context combCtx =
            new WrappedReducer<Text, EventCountWritable, Text, EventCountWritable>().getReducerContext(reduceCtx);

        combiner.reduce(new Text(keyStr), values, combCtx);
        return captured.get(0);
    }

    /**
     * Helper to execute One Combiner Pass before Reducer.
     */
    private String runOneCombinerPass(String keyStr, List<EventCountWritable> rawValues) throws Exception {
        EventCountWritable combinedVal = runCombiner(keyStr, rawValues);
        return runDirectReducer(keyStr, Collections.singletonList(combinedVal));
    }

    /**
     * Helper to execute Multiple Combiner Passes with chunked sub-batches.
     */
    private String runMultiCombinerPasses(String keyStr, List<EventCountWritable> rawValues, int chunkSize) throws Exception {
        List<EventCountWritable> intermediateCombined = new ArrayList<>();

        for (int i = 0; i < rawValues.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, rawValues.size());
            List<EventCountWritable> subList = rawValues.subList(i, end);
            intermediateCombined.add(runCombiner(keyStr, subList));
        }

        // Second combiner pass on intermediate chunks
        EventCountWritable finalCombined = runCombiner(keyStr, intermediateCombined);
        return runDirectReducer(keyStr, Collections.singletonList(finalCombined));
    }

    @Test
    @DisplayName("Combiner Equivalence: Direct Reducer vs 1 Combiner vs Multi-Chunk Combiners")
    void testCombinerEquivalenceMixedEvents() throws Exception {
        String key = "2019-11-01 14";

        List<EventCountWritable> inputList = Arrays.asList(
            new EventCountWritable(1, 0, 0, 0, 0), // view
            new EventCountWritable(1, 0, 0, 0, 0), // view
            new EventCountWritable(0, 1, 0, 0, 0), // cart
            new EventCountWritable(0, 0, 1, 0, 0), // remove
            new EventCountWritable(0, 0, 0, 1, 0), // purchase
            new EventCountWritable(1, 0, 0, 0, 0), // view
            new EventCountWritable(0, 0, 0, 0, 1), // malformed
            new EventCountWritable(0, 1, 0, 0, 0), // cart
            new EventCountWritable(0, 0, 0, 1, 0), // purchase
            new EventCountWritable(0, 0, 0, 0, 1)  // malformed
        );

        String directOut = runDirectReducer(key, inputList);
        String onePassOut = runOneCombinerPass(key, inputList);
        String multiPassOut = runMultiCombinerPasses(key, inputList, 3);

        assertEquals(directOut, onePassOut, "Direct reduction and 1-pass combiner must yield identical output");
        assertEquals(directOut, multiPassOut, "Direct reduction and multi-pass chunked combiner must yield identical output");

        // Verify content: views=3, carts=2, removes=1, purchases=2 -> total=8, malformed=2, error_rate=2/10 = 0.200000
        assertEquals("2019-11-01\t14\t8\t3\t2\t1\t2\t2\t0.200000", directOut);
    }

    @Test
    @DisplayName("Combiner Equivalence: Unattributable INVALID_TIME records")
    void testCombinerEquivalenceUnattributable() throws Exception {
        String key = "INVALID_TIME";

        List<EventCountWritable> inputList = Arrays.asList(
            new EventCountWritable(0, 0, 0, 0, 1),
            new EventCountWritable(0, 0, 0, 0, 1),
            new EventCountWritable(0, 0, 0, 0, 1)
        );

        String directOut = runDirectReducer(key, inputList);
        String onePassOut = runOneCombinerPass(key, inputList);
        String multiPassOut = runMultiCombinerPasses(key, inputList, 2);

        assertEquals(directOut, onePassOut);
        assertEquals(directOut, multiPassOut);
        assertEquals("NA\tNA\t0\t0\t0\t0\t0\t3\t1.000000", directOut);
    }

    @Test
    @DisplayName("Algebraic Associativity and Commutativity Proof for EventCountWritable")
    void testAlgebraicAssociativityAndCommutativity() {
        EventCountWritable a = new EventCountWritable(10, 5, 2, 1, 3);
        EventCountWritable b = new EventCountWritable(25, 12, 4, 3, 1);
        EventCountWritable c = new EventCountWritable(7, 2, 0, 0, 5);

        // (a + b) + c
        EventCountWritable leftAssoc = new EventCountWritable();
        leftAssoc.merge(a);
        leftAssoc.merge(b);
        leftAssoc.merge(c);

        // a + (b + c)
        EventCountWritable bc = new EventCountWritable();
        bc.merge(b);
        bc.merge(c);
        EventCountWritable rightAssoc = new EventCountWritable();
        rightAssoc.merge(a);
        rightAssoc.merge(bc);

        // Commutativity: (c + a) + b
        EventCountWritable commuted = new EventCountWritable();
        commuted.merge(c);
        commuted.merge(a);
        commuted.merge(b);

        assertEquals(leftAssoc, rightAssoc, "Associative property failed: (a + b) + c != a + (b + c)");
        assertEquals(leftAssoc, commuted, "Commutative property failed: (a + b) + c != (c + a) + b");
    }
}
