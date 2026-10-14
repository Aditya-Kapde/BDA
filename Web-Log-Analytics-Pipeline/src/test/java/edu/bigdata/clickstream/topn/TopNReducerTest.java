package edu.bigdata.clickstream.topn;

import org.apache.hadoop.conf.Configuration;
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

import static org.junit.jupiter.api.Assertions.*;

public class TopNReducerTest {

    private TopNReducer reducer;
    private List<String> writtenOutputs;
    private Reducer<NullWritable, Text, NullWritable, Text>.Context context;
    private Configuration conf;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        reducer = new TopNReducer();
        writtenOutputs = new ArrayList<>();
        conf = new Configuration();
        conf.setInt(TopNReducer.CONF_TOPN, 3); // N = 3 for clear boundary testing

        ReduceContext<NullWritable, Text, NullWritable, Text> reduceContext =
            (ReduceContext<NullWritable, Text, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getConfiguration".equals(name)) {
                        return conf;
                    }
                    if ("write".equals(name) && args != null && args.length >= 2) {
                        writtenOutputs.add(args[1].toString());
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedReducer<NullWritable, Text, NullWritable, Text>().getReducerContext(reduceContext);
    }

    @Test
    @DisplayName("Product view ranking produces exact 5-column TSV output schema with 1-indexed ranks")
    void testProductViewRankingSchema() throws Exception {
        reducer.setup(context);

        List<Text> inputs = Arrays.asList(
            new Text("1001\telectronics.smartphone\tacme\t3"),
            new Text("1002\t\t\t1"),
            new Text("1004\telectronics.smartphone\tacme\t1"),
            new Text("1005\telectronics.smartphone\tacme\t1")
        );

        reducer.reduce(NullWritable.get(), inputs, context);

        // Top 3 out of 4:
        // Rank 1: 1001 (views: 3)
        // Rank 2: 1002 (views: 1, key: 1002 < 1004)
        // Rank 3: 1004 (views: 1, key: 1004 < 1005)
        assertEquals(3, writtenOutputs.size());
        assertEquals("1\t1001\telectronics.smartphone\tacme\t3", writtenOutputs.get(0));
        assertEquals("2\t1002\t\t\t1", writtenOutputs.get(1));
        assertEquals("3\t1004\telectronics.smartphone\tacme\t1", writtenOutputs.get(2));
    }

    @Test
    @DisplayName("Category view ranking produces exact 3-column TSV output schema")
    void testCategoryViewRankingSchema() throws Exception {
        conf.setInt(TopNReducer.CONF_TOPN, 2);
        reducer.setup(context);

        List<Text> inputs = Arrays.asList(
            new Text("appliances.kitchen\t20"),
            new Text("electronics.smartphone\t100"),
            new Text("computers.notebook\t50")
        );

        reducer.reduce(NullWritable.get(), inputs, context);

        assertEquals(2, writtenOutputs.size());
        assertEquals("1\telectronics.smartphone\t100", writtenOutputs.get(0));
        assertEquals("2\tcomputers.notebook\t50", writtenOutputs.get(1));
    }

    @Test
    @DisplayName("Deterministic tie-breaking: ties broken strictly by key ascending")
    void testDeterministicTieBreaking() throws Exception {
        conf.setInt(TopNReducer.CONF_TOPN, 5);
        reducer.setup(context);

        List<Text> inputs = Arrays.asList(
            new Text("1009\tcat\tbrand\t10"),
            new Text("1003\tcat\tbrand\t10"),
            new Text("1007\tcat\tbrand\t10"),
            new Text("1001\tcat\tbrand\t10"),
            new Text("1005\tcat\tbrand\t10")
        );

        reducer.reduce(NullWritable.get(), inputs, context);

        assertEquals(5, writtenOutputs.size());
        assertTrue(writtenOutputs.get(0).startsWith("1\t1001\t"));
        assertTrue(writtenOutputs.get(1).startsWith("2\t1003\t"));
        assertTrue(writtenOutputs.get(2).startsWith("3\t1005\t"));
        assertTrue(writtenOutputs.get(3).startsWith("4\t1007\t"));
        assertTrue(writtenOutputs.get(4).startsWith("5\t1009\t"));
    }

    @Test
    @DisplayName("Input order independence: shuffled input yields identical ranked results")
    void testInputOrderIndependence() throws Exception {
        conf.setInt(TopNReducer.CONF_TOPN, 3);

        List<String> rawInputs = Arrays.asList(
            "p1\tcat\tbrand\t500",
            "p2\tcat\tbrand\t400",
            "p3\tcat\tbrand\t300",
            "p4\tcat\tbrand\t200",
            "p5\tcat\tbrand\t100"
        );

        // Run 1: original order
        reducer.setup(context);
        List<Text> textInputs1 = new ArrayList<>();
        for (String r : rawInputs) textInputs1.add(new Text(r));
        reducer.reduce(NullWritable.get(), textInputs1, context);
        List<String> run1 = new ArrayList<>(writtenOutputs);

        // Run 2: reversed order
        writtenOutputs.clear();
        reducer.setup(context);
        List<Text> textInputs2 = new ArrayList<>(textInputs1);
        Collections.reverse(textInputs2);
        reducer.reduce(NullWritable.get(), textInputs2, context);
        List<String> run2 = new ArrayList<>(writtenOutputs);

        // Run 3: shuffled order
        writtenOutputs.clear();
        reducer.setup(context);
        List<Text> textInputs3 = new ArrayList<>(textInputs1);
        Collections.shuffle(textInputs3);
        reducer.reduce(NullWritable.get(), textInputs3, context);
        List<String> run3 = new ArrayList<>(writtenOutputs);

        assertEquals(run1, run2);
        assertEquals(run1, run3);
        assertEquals(3, run1.size());
    }

    @Test
    @DisplayName("Empty or corrupted inputs emit nothing")
    void testEmptyAndCorruptedInputs() throws Exception {
        reducer.setup(context);

        List<Text> inputs = Arrays.asList(
            new Text(""),
            new Text("invalid_single_col"),
            new Text("1001\tnot_a_num")
        );

        reducer.reduce(NullWritable.get(), inputs, context);
        assertTrue(writtenOutputs.isEmpty());
    }
}
