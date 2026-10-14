package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.DailyFunnelWritable;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DailyFunnelMapperTest {

    private DailyFunnelMapper mapper;
    private Map<String, Long> counterValues;
    private List<String> writtenKeys;
    private List<DailyFunnelWritable> writtenValues;
    private Mapper<LongWritable, Text, Text, DailyFunnelWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new DailyFunnelMapper();
        counterValues = new HashMap<>();
        writtenKeys = new ArrayList<>();
        writtenValues = new ArrayList<>();

        MapContext<LongWritable, Text, Text, DailyFunnelWritable> mapContext =
            (MapContext<LongWritable, Text, Text, DailyFunnelWritable>) Proxy.newProxyInstance(
                MapContext.class.getClassLoader(),
                new Class<?>[]{MapContext.class},
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
                        writtenKeys.add(args[0].toString());
                        DailyFunnelWritable copy = new DailyFunnelWritable();
                        DailyFunnelWritable src = (DailyFunnelWritable) args[1];
                        copy.set(src.getSessions(), src.getCart(), src.getPurchase(), src.getAbandoned(), src.getRemoves());
                        writtenValues.add(copy);
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedMapper<LongWritable, Text, Text, DailyFunnelWritable>().getMapContext(mapContext);
    }

    @Test
    @DisplayName("Valid J5 partial row emits date key and correct DailyFunnelWritable values")
    void testValidRow() throws Exception {
        Text line = new Text("2019-11-01\t6\t2\t1\t1\t1");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        assertEquals("2019-11-01", writtenKeys.get(0));

        DailyFunnelWritable val = writtenValues.get(0);
        assertEquals(6L, val.getSessions());
        assertEquals(2L, val.getCart());
        assertEquals(1L, val.getPurchase());
        assertEquals(1L, val.getAbandoned());
        assertEquals(1L, val.getRemoves());

        assertEquals(1L, counterValues.get(QualityCounter.TOTAL_RECORDS.name()));
        assertEquals(1L, counterValues.get(QualityCounter.VALID_RECORDS.name()));
    }

    @Test
    @DisplayName("Blank and empty lines are skipped safely")
    void testBlankLinesSkipped() throws Exception {
        mapper.map(new LongWritable(0), new Text("   "), context);
        mapper.map(new LongWritable(0), new Text(""), context);

        assertTrue(writtenKeys.isEmpty());
        assertTrue(counterValues.isEmpty());
    }

    @Test
    @DisplayName("Malformed column count is skipped and increments MALFORMED_COLUMN_COUNT")
    void testMalformedColumnCount() throws Exception {
        // Fewer than 6 columns
        mapper.map(new LongWritable(0), new Text("2019-11-01\t6\t2"), context);
        // More than 6 columns
        mapper.map(new LongWritable(0), new Text("2019-11-01\t6\t2\t1\t1\t1\textra"), context);

        assertTrue(writtenKeys.isEmpty());
        assertEquals(2L, counterValues.get(QualityCounter.TOTAL_RECORDS.name()));
        assertEquals(2L, counterValues.get(QualityCounter.MALFORMED_COLUMN_COUNT.name()));
    }

    @Test
    @DisplayName("Invalid date formats increment MALFORMED_TIMESTAMP and emit nothing")
    void testInvalidDates() throws Exception {
        LongWritable key = new LongWritable(0);

        // Invalid month
        mapper.map(key, new Text("2019-13-01\t6\t2\t1\t1\t1"), context);
        // Non-leap year Feb 29
        mapper.map(key, new Text("2019-02-29\t6\t2\t1\t1\t1"), context);
        // Bad format
        mapper.map(key, new Text("not-a-date\t6\t2\t1\t1\t1"), context);

        assertTrue(writtenKeys.isEmpty());
        assertEquals(3L, counterValues.get(QualityCounter.MALFORMED_TIMESTAMP.name()));
    }

    @Test
    @DisplayName("Non-numeric or negative metric fields increment MALFORMED_ID and emit nothing")
    void testInvalidMetrics() throws Exception {
        LongWritable key = new LongWritable(0);

        // Non-numeric field
        mapper.map(key, new Text("2019-11-01\tABC\t2\t1\t1\t1"), context);
        // Negative count
        mapper.map(key, new Text("2019-11-01\t6\t-2\t1\t1\t1"), context);

        assertTrue(writtenKeys.isEmpty());
        assertEquals(2L, counterValues.get(QualityCounter.MALFORMED_ID.name()));
    }

    @Test
    @DisplayName("Zero-valued metrics and large numbers are parsed correctly")
    void testZeroAndLargeMetrics() throws Exception {
        Text line = new Text("2019-11-01\t0\t0\t0\t0\t0");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        DailyFunnelWritable val = writtenValues.get(0);
        assertEquals(0L, val.getSessions());
        assertEquals(0L, val.getCart());

        Text largeLine = new Text("2019-11-02\t1000000000\t500000000\t250000000\t250000000\t100000000");
        mapper.map(new LongWritable(0), largeLine, context);

        assertEquals(2, writtenKeys.size());
        DailyFunnelWritable largeVal = writtenValues.get(1);
        assertEquals(1000000000L, largeVal.getSessions());
        assertEquals(500000000L, largeVal.getCart());
    }
}
