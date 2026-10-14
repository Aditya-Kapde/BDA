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

public class ViewCountReducerTest {

    private ViewCountReducer reducer;
    private Map<String, List<String>> recordedOutputs;
    private Map<String, List<String>> recordedPaths;
    private boolean closed;
    private Reducer<Text, ViewCountWritable, NullWritable, Text>.Context dummyContext;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        recordedOutputs = new HashMap<>();
        recordedPaths = new HashMap<>();
        closed = false;

        ViewCountReducer.OutputCollector testCollector = new ViewCountReducer.OutputCollector() {
            @Override
            public void write(String namedOutput, Text value, String baseOutputPath) {
                recordedOutputs.computeIfAbsent(namedOutput, k -> new ArrayList<>()).add(value.toString());
                recordedPaths.computeIfAbsent(namedOutput, k -> new ArrayList<>()).add(baseOutputPath);
            }

            @Override
            public void close() {
                closed = true;
            }
        };

        reducer = new ViewCountReducer(testCollector);

        ReduceContext<Text, ViewCountWritable, NullWritable, Text> dummyReduceContext =
            (ReduceContext<Text, ViewCountWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> null
            );
        dummyContext = new WrappedReducer<Text, ViewCountWritable, NullWritable, Text>().getReducerContext(dummyReduceContext);
    }

    @Test
    @DisplayName("Product view reduction formats exact 4-column schema under products/part")
    void testProductViewReduction() throws Exception {
        Text key = new Text("P|1001");
        List<ViewCountWritable> values = Arrays.asList(
            new ViewCountWritable(1, "electronics.smartphone", "acme"),
            new ViewCountWritable(1, "electronics.smartphone", "acme"),
            new ViewCountWritable(1, "electronics.smartphone", "acme")
        );

        reducer.reduce(key, values, dummyContext);

        List<String> prodLines = recordedOutputs.get(ViewCountReducer.OUTPUT_PRODUCTS);
        assertNotNull(prodLines);
        assertEquals(1, prodLines.size());
        assertEquals("1001\telectronics.smartphone\tacme\t3", prodLines.get(0));

        // Verify baseOutputPath is "products/part"
        assertEquals("products/part", recordedPaths.get(ViewCountReducer.OUTPUT_PRODUCTS).get(0));
    }

    @Test
    @DisplayName("Category view reduction formats exact 2-column schema under categories/part")
    void testCategoryViewReduction() throws Exception {
        Text key = new Text("C|electronics.smartphone");
        List<ViewCountWritable> values = Arrays.asList(
            new ViewCountWritable(2, "", ""),
            new ViewCountWritable(3, "", "")
        );

        reducer.reduce(key, values, dummyContext);

        List<String> catLines = recordedOutputs.get(ViewCountReducer.OUTPUT_CATEGORIES);
        assertNotNull(catLines);
        assertEquals(1, catLines.size());
        assertEquals("electronics.smartphone\t5", catLines.get(0));

        // Verify baseOutputPath is "categories/part"
        assertEquals("categories/part", recordedPaths.get(ViewCountReducer.OUTPUT_CATEGORIES).get(0));
    }

    @Test
    @DisplayName("Multiple products and categories route to respective named outputs without cross-contamination")
    void testMultipleEntitiesAndRoutingIsolation() throws Exception {
        reducer.reduce(new Text("P|1001"), Collections.singletonList(new ViewCountWritable(3, "cat_a", "brand_a")), dummyContext);
        reducer.reduce(new Text("P|1002"), Collections.singletonList(new ViewCountWritable(1, "", "")), dummyContext);
        reducer.reduce(new Text("C|cat_a"), Collections.singletonList(new ViewCountWritable(3, "", "")), dummyContext);
        reducer.reduce(new Text("C|cat_b"), Collections.singletonList(new ViewCountWritable(7, "", "")), dummyContext);

        List<String> products = recordedOutputs.get(ViewCountReducer.OUTPUT_PRODUCTS);
        List<String> categories = recordedOutputs.get(ViewCountReducer.OUTPUT_CATEGORIES);

        assertEquals(2, products.size());
        assertEquals("1001\tcat_a\tbrand_a\t3", products.get(0));
        assertEquals("1002\t\t\t1", products.get(1));

        assertEquals(2, categories.size());
        assertEquals("cat_a\t3", categories.get(0));
        assertEquals("cat_b\t7", categories.get(1));
    }

    @Test
    @DisplayName("Zero count views emit nothing")
    void testZeroCountEmitsNothing() throws Exception {
        reducer.reduce(new Text("P|9999"), Collections.singletonList(new ViewCountWritable(0, "cat", "brand")), dummyContext);
        assertNull(recordedOutputs.get(ViewCountReducer.OUTPUT_PRODUCTS));
    }

    @Test
    @DisplayName("cleanup calls close on collector")
    void testCleanupLifecycle() throws Exception {
        assertFalse(closed);
        reducer.cleanup(dummyContext);
        assertTrue(closed);
    }
}
