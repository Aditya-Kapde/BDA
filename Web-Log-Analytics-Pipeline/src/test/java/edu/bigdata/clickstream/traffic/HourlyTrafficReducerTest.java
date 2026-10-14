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

public class HourlyTrafficReducerTest {

    private HourlyTrafficReducer reducer;
    private List<String> writtenLines;
    private Reducer<Text, EventCountWritable, NullWritable, Text>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        reducer = new HourlyTrafficReducer();
        writtenLines = new ArrayList<>();

        ReduceContext<Text, EventCountWritable, NullWritable, Text> reduceContext =
            (ReduceContext<Text, EventCountWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        writtenLines.add(args[1].toString());
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedReducer<Text, EventCountWritable, NullWritable, Text>().getReducerContext(reduceContext);
    }

    @Test
    @DisplayName("Reducer formats regular date-hour records per SRS §3.3.4 schema")
    void testRegularDateHourOutput() throws Exception {
        Text key = new Text("2019-11-01 01");
        // Row 1: view 1
        // Row 2: purchase 1
        // Row 3: malformed 1 (attributable)
        Iterable<EventCountWritable> values = Arrays.asList(
            new EventCountWritable(1, 0, 0, 0, 0),
            new EventCountWritable(0, 0, 0, 1, 0),
            new EventCountWritable(0, 0, 0, 0, 1)
        );

        reducer.reduce(key, values, context);

        assertEquals(1, writtenLines.size());
        // total = 1+1=2, malformed = 1, error_rate = 1/3 = 0.333333
        String expected = "2019-11-01\t1\t2\t1\t0\t0\t1\t1\t0.333333";
        assertEquals(expected, writtenLines.get(0));
    }

    @Test
    @DisplayName("Reducer formats INVALID_TIME records per SRS §3.3.4")
    void testInvalidTimeOutput() throws Exception {
        Text key = new Text("INVALID_TIME");
        Iterable<EventCountWritable> values = Arrays.asList(
            new EventCountWritable(0, 0, 0, 0, 1),
            new EventCountWritable(0, 0, 0, 0, 1)
        );

        reducer.reduce(key, values, context);

        assertEquals(1, writtenLines.size());
        String expected = "NA\tNA\t0\t0\t0\t0\t0\t2\t1.000000";
        assertEquals(expected, writtenLines.get(0));
    }

    @Test
    @DisplayName("Reducer handles zero-error scenario correctly")
    void testZeroErrorScenario() throws Exception {
        Text key = new Text("2019-11-01 00");
        Iterable<EventCountWritable> values = Collections.singletonList(
            new EventCountWritable(5, 2, 1, 1, 0)
        );

        reducer.reduce(key, values, context);

        assertEquals(1, writtenLines.size());
        // total = 9, malformed = 0, error_rate = 0.000000
        String expected = "2019-11-01\t0\t9\t5\t2\t1\t1\t0\t0.000000";
        assertEquals(expected, writtenLines.get(0));
    }
}
