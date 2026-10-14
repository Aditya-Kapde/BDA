package edu.bigdata.clickstream.peakhour;

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

public class PeakHourReducerTest {

    private PeakHourReducer reducer;
    private Map<String, List<String>> recordedOutputs;
    private boolean closed;
    private Reducer<Text, Text, NullWritable, Text>.Context dummyContext;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        recordedOutputs = new HashMap<>();
        closed = false;

        PeakHourReducer.OutputCollector testCollector = new PeakHourReducer.OutputCollector() {
            @Override
            public void write(String namedOutput, Text value) {
                recordedOutputs.computeIfAbsent(namedOutput, k -> new ArrayList<>()).add(value.toString());
            }

            @Override
            public void close() {
                closed = true;
            }
        };

        reducer = new PeakHourReducer(testCollector);

        ReduceContext<Text, Text, NullWritable, Text> dummyReduceContext =
            (ReduceContext<Text, Text, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> null
            );
        dummyContext = new WrappedReducer<Text, Text, NullWritable, Text>().getReducerContext(dummyReduceContext);
    }

    @Test
    @DisplayName("peakday correctly computes peak_hour, peak_total, and day_total")
    void testPeakDayNormalCalculation() throws Exception {
        Text key = new Text("D|2019-11-01");
        List<Text> values = Arrays.asList(
            new Text("0:10"),
            new Text("1:25"),
            new Text("2:5"),
            new Text("3:12")
        );

        reducer.reduce(key, values, dummyContext);

        List<String> peakDayLines = recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY);
        assertNotNull(peakDayLines);
        assertEquals(1, peakDayLines.size());

        // date \t peak_hour \t peak_total \t day_total
        // max = 25 at hour 1, total = 10+25+5+12 = 52
        assertEquals("2019-11-01\t1\t25\t52", peakDayLines.get(0));
    }

    @Test
    @DisplayName("peakday applies exact tie-breaking rule: smallest hour wins when totals are equal")
    void testPeakDayTieBreaking() throws Exception {
        Text key = new Text("D|2019-11-01");
        // Hour 5 has 30, Hour 2 has 30, Hour 10 has 30, Hour 0 has 29
        List<Text> values = Arrays.asList(
            new Text("5:30"),
            new Text("2:30"),
            new Text("10:30"),
            new Text("0:29")
        );

        reducer.reduce(key, values, dummyContext);

        List<String> peakDayLines = recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY);
        assertNotNull(peakDayLines);
        assertEquals(1, peakDayLines.size());

        // Among hours 2, 5, 10 with max total 30, smallest hour (2) must win!
        // day_total = 30 + 30 + 30 + 29 = 119
        assertEquals("2019-11-01\t2\t30\t119", peakDayLines.get(0));
    }

    @Test
    @DisplayName("Deterministic output under shuffled input order")
    void testDeterministicUnderShuffle() throws Exception {
        Text key = new Text("D|2019-11-01");
        List<Text> values1 = Arrays.asList(new Text("0:15"), new Text("1:40"), new Text("2:40"), new Text("3:10"));
        List<Text> values2 = Arrays.asList(new Text("3:10"), new Text("2:40"), new Text("0:15"), new Text("1:40"));

        reducer.reduce(key, values1, dummyContext);
        String res1 = recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY).get(0);

        recordedOutputs.clear();
        reducer.reduce(key, values2, dummyContext);
        String res2 = recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY).get(0);

        assertEquals(res1, res2);
        // Tie between hour 1 and 2 with 40 -> hour 1 wins
        assertEquals("2019-11-01\t1\t40\t105", res1);
    }

    @Test
    @DisplayName("Multiple distinct dates produce separate peakday records")
    void testMultipleDates() throws Exception {
        reducer.reduce(new Text("D|2019-11-01"), Arrays.asList(new Text("0:5"), new Text("1:15")), dummyContext);
        reducer.reduce(new Text("D|2019-11-02"), Arrays.asList(new Text("0:20"), new Text("1:10")), dummyContext);

        List<String> peakDayLines = recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY);
        assertEquals(2, peakDayLines.size());
        assertEquals("2019-11-01\t1\t15\t20", peakDayLines.get(0));
        assertEquals("2019-11-02\t0\t20\t30", peakDayLines.get(1));
    }

    @Test
    @DisplayName("hourofday correctly aggregates total_events and days_observed")
    void testHourOfDayAggregation() throws Exception {
        Text key = new Text("H|14");
        List<Text> values = Arrays.asList(
            new Text("2019-11-01:100"),
            new Text("2019-11-02:150"),
            new Text("2019-11-03:50")
        );

        reducer.reduce(key, values, dummyContext);

        List<String> hodLines = recordedOutputs.get(PeakHourReducer.OUTPUT_HOUROFDAY);
        assertNotNull(hodLines);
        assertEquals(1, hodLines.size());

        // hour \t total_events \t days_observed
        // total_events = 100 + 150 + 50 = 300, days_observed = 3
        assertEquals("14\t300\t3", hodLines.get(0));
    }

    @Test
    @DisplayName("hourofday with single-digit padded key extracts integer hour correctly")
    void testHourOfDayPaddedKey() throws Exception {
        Text key = new Text("H|02");
        List<Text> values = Collections.singletonList(new Text("2019-11-01:42"));

        reducer.reduce(key, values, dummyContext);

        List<String> hodLines = recordedOutputs.get(PeakHourReducer.OUTPUT_HOUROFDAY);
        assertEquals(1, hodLines.size());
        assertEquals("2\t42\t1", hodLines.get(0));
    }

    @Test
    @DisplayName("MultipleOutputs routing isolation: peakday and hourofday records do not cross-contaminate")
    void testRoutingIsolation() throws Exception {
        reducer.reduce(new Text("D|2019-11-01"), Collections.singletonList(new Text("0:10")), dummyContext);
        reducer.reduce(new Text("H|00"), Collections.singletonList(new Text("2019-11-01:10")), dummyContext);

        List<String> peakDay = recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY);
        List<String> hod = recordedOutputs.get(PeakHourReducer.OUTPUT_HOUROFDAY);

        assertEquals(1, peakDay.size());
        assertEquals("2019-11-01\t0\t10\t10", peakDay.get(0));

        assertEquals(1, hod.size());
        assertEquals("0\t10\t1", hod.get(0));
    }

    @Test
    @DisplayName("Empty or corrupted values emit nothing")
    void testEmptyOrCorruptedValues() throws Exception {
        Text key = new Text("D|2019-11-01");
        List<Text> badValues = Arrays.asList(new Text("bad_format"), new Text(""));

        reducer.reduce(key, badValues, dummyContext);

        assertNull(recordedOutputs.get(PeakHourReducer.OUTPUT_PEAKDAY));
    }

    @Test
    @DisplayName("cleanup closes collector properly")
    void testCleanupLifecycle() throws Exception {
        assertFalse(closed);
        reducer.cleanup(dummyContext);
        assertTrue(closed);
    }
}
