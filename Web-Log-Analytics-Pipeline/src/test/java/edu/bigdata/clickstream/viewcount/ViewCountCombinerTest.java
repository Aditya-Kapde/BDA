package edu.bigdata.clickstream.viewcount;

import edu.bigdata.clickstream.writable.ViewCountWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ViewCountCombinerTest {

    private ViewCountCombiner combiner;
    private ViewCountReducer reducer;

    @BeforeEach
    void setUp() {
        combiner = new ViewCountCombiner();
        reducer = new ViewCountReducer();
    }

    /**
     * Executes Reducer directly on raw values and returns collected string output.
     */
    @SuppressWarnings("unchecked")
    private String runDirectReducer(String keyStr, List<ViewCountWritable> rawValues) throws Exception {
        final List<String> captured = new ArrayList<>();
        ViewCountReducer.OutputCollector testCollector = new ViewCountReducer.OutputCollector() {
            @Override
            public void write(String namedOutput, Text value, String baseOutputPath) {
                captured.add(namedOutput + ":" + baseOutputPath + ":" + value.toString());
            }

            @Override
            public void close() {
            }
        };

        ViewCountReducer red = new ViewCountReducer(testCollector);
        ReduceContext<Text, ViewCountWritable, NullWritable, Text> dummyContext =
            (ReduceContext<Text, ViewCountWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> null
            );
        Reducer<Text, ViewCountWritable, NullWritable, Text>.Context ctx =
            new WrappedReducer<Text, ViewCountWritable, NullWritable, Text>().getReducerContext(dummyContext);

        red.reduce(new Text(keyStr), rawValues, ctx);
        return captured.isEmpty() ? "" : captured.get(0);
    }

    /**
     * Runs Combiner on a batch and returns snapshot of combined ViewCountWritable.
     */
    @SuppressWarnings("unchecked")
    private ViewCountWritable runCombiner(String keyStr, List<ViewCountWritable> values) throws Exception {
        final List<ViewCountWritable> captured = new ArrayList<>();
        ReduceContext<Text, ViewCountWritable, Text, ViewCountWritable> redContext =
            (ReduceContext<Text, ViewCountWritable, Text, ViewCountWritable>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        ViewCountWritable v = (ViewCountWritable) args[1];
                        captured.add(new ViewCountWritable(v.getCount(), v.getCategory(), v.getBrand()));
                    }
                    return null;
                }
            );

        Reducer<Text, ViewCountWritable, Text, ViewCountWritable>.Context ctx =
            new WrappedReducer<Text, ViewCountWritable, Text, ViewCountWritable>().getReducerContext(redContext);

        combiner.reduce(new Text(keyStr), values, ctx);
        return captured.get(0);
    }

    @Test
    @DisplayName("Combiner Equivalence: Direct Reducer vs 1 Combiner vs Multi-Chunk Combiners")
    void testCombinerEquivalence() throws Exception {
        String key = "P|1001";
        List<ViewCountWritable> inputs = Arrays.asList(
            new ViewCountWritable(1, "electronics.smartphone", "samsung"),
            new ViewCountWritable(1, "electronics.audio", "apple"),
            new ViewCountWritable(1, "", "xiaomi"),
            new ViewCountWritable(1, "appliances.kitchen", "")
        );

        // Path 1: Direct Reducer
        String directOut = runDirectReducer(key, inputs);

        // Path 2: 1 Combiner Pass + Reducer
        ViewCountWritable onePass = runCombiner(key, inputs);
        String onePassOut = runDirectReducer(key, Collections.singletonList(onePass));

        // Path 3: Multi-Chunk Combiner Passes (chunks of 2) + Reducer
        List<ViewCountWritable> chunkedIntermediate = Arrays.asList(
            runCombiner(key, inputs.subList(0, 2)),
            runCombiner(key, inputs.subList(2, 4))
        );
        ViewCountWritable multiPass = runCombiner(key, chunkedIntermediate);
        String multiPassOut = runDirectReducer(key, Collections.singletonList(multiPass));

        assertEquals(directOut, onePassOut, "1-pass combiner must match direct reduction");
        assertEquals(directOut, multiPassOut, "Multi-chunk combiner must match direct reduction");

        // Deterministic metadata verification:
        // count = 4
        // category: min of ("electronics.smartphone", "electronics.audio", "appliances.kitchen") -> "appliances.kitchen"
        // brand: min of ("samsung", "apple", "xiaomi") -> "apple"
        assertEquals("products:products/part:1001\tappliances.kitchen\tapple\t4", directOut);
    }

    @Test
    @DisplayName("Algebraic Associativity and Commutativity Proof for ViewCountWritable")
    void testAlgebraicProperties() {
        ViewCountWritable a = new ViewCountWritable(5, "cat_b", "brand_z");
        ViewCountWritable b = new ViewCountWritable(10, "cat_a", "brand_y");
        ViewCountWritable c = new ViewCountWritable(2, "cat_c", "brand_x");

        // (a + b) + c
        ViewCountWritable leftAssoc = new ViewCountWritable();
        leftAssoc.merge(a);
        leftAssoc.merge(b);
        leftAssoc.merge(c);

        // a + (b + c)
        ViewCountWritable bc = new ViewCountWritable();
        bc.merge(b);
        bc.merge(c);
        ViewCountWritable rightAssoc = new ViewCountWritable();
        rightAssoc.merge(a);
        rightAssoc.merge(bc);

        // Commutativity: (c + a) + b
        ViewCountWritable commuted = new ViewCountWritable();
        commuted.merge(c);
        commuted.merge(a);
        commuted.merge(b);

        assertEquals(leftAssoc, rightAssoc, "Associative property failed");
        assertEquals(leftAssoc, commuted, "Commutative property failed");

        assertEquals(17L, leftAssoc.getCount());
        assertEquals("cat_a", leftAssoc.getCategory());
        assertEquals("brand_x", leftAssoc.getBrand());
    }

    @Test
    @DisplayName("Input-order independence: shuffled inputs yield identical merged result")
    void testShuffledInputOrder() throws Exception {
        String key = "P|1001";
        List<ViewCountWritable> list1 = Arrays.asList(
            new ViewCountWritable(1, "z_cat", "z_brand"),
            new ViewCountWritable(2, "a_cat", "m_brand"),
            new ViewCountWritable(3, "m_cat", "a_brand")
        );

        List<ViewCountWritable> list2 = Arrays.asList(
            new ViewCountWritable(3, "m_cat", "a_brand"),
            new ViewCountWritable(1, "z_cat", "z_brand"),
            new ViewCountWritable(2, "a_cat", "m_brand")
        );

        String out1 = runDirectReducer(key, list1);
        String out2 = runDirectReducer(key, list2);

        assertEquals(out1, out2);
        assertEquals("products:products/part:1001\ta_cat\ta_brand\t6", out1);
    }
}
