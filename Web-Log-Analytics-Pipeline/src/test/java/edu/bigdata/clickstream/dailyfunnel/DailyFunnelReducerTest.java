package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.DailyFunnelWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DailyFunnelReducerTest {

    private DailyFunnelReducer reducer;
    private List<String> capturedOutputs;
    private Map<String, Long> counterValues;
    private Reducer<Text, DailyFunnelWritable, NullWritable, Text>.Context testContext;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        reducer = new DailyFunnelReducer();
        capturedOutputs = new ArrayList<>();
        counterValues = new HashMap<>();

        ReduceContext<Text, DailyFunnelWritable, NullWritable, Text> reduceCtx =
            (ReduceContext<Text, DailyFunnelWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getCounter".equals(name) && args != null && args.length > 0) {
                        String counterKey = args[args.length - 1].toString();
                        return (Counter) Proxy.newProxyInstance(
                            Counter.class.getClassLoader(),
                            new Class<?>[]{Counter.class},
                            (p, m, a) -> {
                                if ("increment".equals(m.getName()) && a != null && a.length == 1) {
                                    long inc = ((Number) a[0]).longValue();
                                    counterValues.put(counterKey, counterValues.getOrDefault(counterKey, 0L) + inc);
                                }
                                return null;
                            }
                        );
                    }
                    if ("write".equals(name) && args != null && args.length >= 2) {
                        capturedOutputs.add(args[1].toString());
                    }
                    return null;
                }
            );

        testContext = new WrappedReducer<Text, DailyFunnelWritable, NullWritable, Text>().getReducerContext(reduceCtx);
    }

    @Test
    @DisplayName("Single partial row emits exact 6-column TSV output")
    void testSinglePartialRow() throws Exception {
        Text date = new Text("2019-11-01");
        List<DailyFunnelWritable> values = Collections.singletonList(
            new DailyFunnelWritable(6L, 2L, 1L, 1L, 1L)
        );

        reducer.reduce(date, values, testContext);

        assertEquals(1, capturedOutputs.size());
        assertEquals("2019-11-01\t6\t2\t1\t1\t1", capturedOutputs.get(0));
        assertEquals(1L, counterValues.get(QualityCounter.DATES_EMITTED.name()));
    }

    @Test
    @DisplayName("Multiple partial rows for the same date are consolidated into exactly one row")
    void testMultiplePartialRowsConsolidation() throws Exception {
        Text date = new Text("2019-11-01");
        List<DailyFunnelWritable> values = Arrays.asList(
            new DailyFunnelWritable(4L, 1L, 1L, 0L, 0L),
            new DailyFunnelWritable(2L, 1L, 0L, 1L, 1L)
        );

        reducer.reduce(date, values, testContext);

        assertEquals(1, capturedOutputs.size());
        // sessions=6, cart=2, purchase=1, abandoned=1, removes=1
        assertEquals("2019-11-01\t6\t2\t1\t1\t1", capturedOutputs.get(0));
        assertEquals(1L, counterValues.get(QualityCounter.DATES_EMITTED.name()));
    }

    @Test
    @DisplayName("Multiple dates each produce exactly one consolidated record")
    void testMultipleDates() throws Exception {
        reducer.reduce(new Text("2019-11-01"), Arrays.asList(
            new DailyFunnelWritable(10L, 4L, 2L, 2L, 1L),
            new DailyFunnelWritable(5L, 2L, 1L, 1L, 0L)
        ), testContext);

        reducer.reduce(new Text("2019-11-02"), Collections.singletonList(
            new DailyFunnelWritable(20L, 8L, 5L, 3L, 2L)
        ), testContext);

        assertEquals(2, capturedOutputs.size());
        assertEquals("2019-11-01\t15\t6\t3\t3\t1", capturedOutputs.get(0));
        assertEquals("2019-11-02\t20\t8\t5\t3\t2", capturedOutputs.get(1));
        assertEquals(2L, counterValues.get(QualityCounter.DATES_EMITTED.name()));
    }

    @Test
    @DisplayName("Zero-valued metrics are consolidated accurately")
    void testZeroMetrics() throws Exception {
        reducer.reduce(new Text("2019-11-01"), Arrays.asList(
            new DailyFunnelWritable(0L, 0L, 0L, 0L, 0L),
            new DailyFunnelWritable(0L, 0L, 0L, 0L, 0L)
        ), testContext);

        assertEquals(1, capturedOutputs.size());
        assertEquals("2019-11-01\t0\t0\t0\t0\t0", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Deterministic output under shuffled input order")
    void testShuffledInputOrder() throws Exception {
        List<DailyFunnelWritable> original = Arrays.asList(
            new DailyFunnelWritable(100L, 40L, 25L, 15L, 5L),
            new DailyFunnelWritable(200L, 80L, 50L, 30L, 10L),
            new DailyFunnelWritable(50L, 20L, 15L, 5L, 2L)
        );

        reducer.reduce(new Text("2019-11-01"), original, testContext);
        String normalOut = capturedOutputs.get(0);

        capturedOutputs.clear();
        List<DailyFunnelWritable> shuffled = new ArrayList<>(original);
        Collections.reverse(shuffled);

        reducer.reduce(new Text("2019-11-01"), shuffled, testContext);
        String shuffledOut = capturedOutputs.get(0);

        assertEquals(normalOut, shuffledOut);
        assertEquals("2019-11-01\t350\t140\t90\t50\t17", normalOut);
    }
}
