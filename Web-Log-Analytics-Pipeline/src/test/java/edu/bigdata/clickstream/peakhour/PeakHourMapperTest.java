package edu.bigdata.clickstream.peakhour;

import edu.bigdata.clickstream.common.QualityCounter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class PeakHourMapperTest {

    private PeakHourMapper mapper;
    private Mapper<LongWritable, Text, Text, Text>.Context context;
    private Map<String, Long> counterValues;
    private List<Map.Entry<String, String>> emitted;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new PeakHourMapper();
        counterValues = new HashMap<>();
        emitted = new ArrayList<>();

        MapContext<LongWritable, Text, Text, Text> mapContext =
            (MapContext<LongWritable, Text, Text, Text>) Proxy.newProxyInstance(
                MapContext.class.getClassLoader(),
                new Class<?>[]{MapContext.class},
                (proxy, method, args) -> {
                    String mName = method.getName();
                    if ("getCounter".equals(mName) && args != null && args.length >= 2) {
                        String group = args[0].toString();
                        String name = args[1].toString();
                        String key = group + ":" + name;
                        return Proxy.newProxyInstance(
                            Counter.class.getClassLoader(),
                            new Class<?>[]{Counter.class},
                            (cProxy, cMethod, cArgs) -> {
                                if ("increment".equals(cMethod.getName()) && cArgs != null && cArgs.length >= 1) {
                                    long incr = ((Number) cArgs[0]).longValue();
                                    counterValues.put(key, counterValues.getOrDefault(key, 0L) + incr);
                                } else if ("getValue".equals(cMethod.getName())) {
                                    return counterValues.getOrDefault(key, 0L);
                                }
                                return null;
                            }
                        );
                    } else if ("write".equals(mName) && args != null && args.length >= 2) {
                        String k = args[0].toString();
                        String v = args[1].toString();
                        emitted.add(new AbstractMap.SimpleEntry<>(k, v));
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedMapper<LongWritable, Text, Text, Text>().getMapContext(mapContext);
    }

    private void verifyCounter(QualityCounter counter, long expected) {
        String key = QualityCounter.GROUP_NAME + ":" + counter.name();
        long actual = counterValues.getOrDefault(key, 0L);
        assertEquals(expected, actual, "Counter mismatch for " + counter.name());
    }

    @Test
    @DisplayName("Valid J1 row emits both D|date -> hour:total and H|hour -> date:total")
    void testValidJ1Row() throws Exception {
        // J1 format: date \t hour \t total \t views \t carts \t removes \t purchases \t malformed \t error_rate
        String line = "2019-11-01\t1\t2\t1\t0\t0\t1\t1\t0.333333";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertEquals(2, emitted.size());

        // Daily peak hour key
        assertEquals("D|2019-11-01", emitted.get(0).getKey());
        assertEquals("1:2", emitted.get(0).getValue());

        // Hour-of-day key (zero-padded 2-digit hour)
        assertEquals("H|01", emitted.get(1).getKey());
        assertEquals("2019-11-01:2", emitted.get(1).getValue());

        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.VALID_RECORDS, 1);
    }

    @Test
    @DisplayName("INVALID_TIME / NA row is skipped and emits nothing")
    void testSkipNaRow() throws Exception {
        String line = "NA\tNA\t0\t0\t0\t0\t0\t2\t1.000000";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertTrue(emitted.isEmpty(), "NA row must not produce any mapper output");
        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.VALID_RECORDS, 0);
    }

    @Test
    @DisplayName("Blank or empty line emits nothing and records no counters")
    void testEmptyLine() throws Exception {
        mapper.map(new LongWritable(0), new Text("   "), context);
        assertTrue(emitted.isEmpty());
        verifyCounter(QualityCounter.TOTAL_RECORDS, 0);
    }

    @Test
    @DisplayName("Row with fewer than 3 columns increments MALFORMED_COLUMN_COUNT and emits nothing")
    void testInsufficientColumns() throws Exception {
        String line = "2019-11-01\t1";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertTrue(emitted.isEmpty());
        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.MALFORMED_COLUMN_COUNT, 1);
    }

    @Test
    @DisplayName("Invalid numeric hour increments MALFORMED_TIMESTAMP")
    void testInvalidHour() throws Exception {
        String line = "2019-11-01\t25\t10\t5\t2\t1\t2\t0\t0.000000";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertTrue(emitted.isEmpty());
        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.MALFORMED_TIMESTAMP, 1);
    }

    @Test
    @DisplayName("Invalid numeric total increments MALFORMED_ID")
    void testInvalidTotal() throws Exception {
        String line = "2019-11-01\t5\tnot_a_number\t5\t2\t1\t2\t0\t0.000000";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertTrue(emitted.isEmpty());
        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.MALFORMED_ID, 1);
    }
}
