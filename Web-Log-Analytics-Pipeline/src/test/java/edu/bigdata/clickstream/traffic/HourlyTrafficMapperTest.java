package edu.bigdata.clickstream.traffic;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.EventCountWritable;
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

public class HourlyTrafficMapperTest {

    private HourlyTrafficMapper mapper;
    private Mapper<LongWritable, Text, Text, EventCountWritable>.Context context;
    private Map<String, Long> counterValues;
    private List<Map.Entry<String, EventCountWritable>> emitted;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new HourlyTrafficMapper();
        counterValues = new HashMap<>();
        emitted = new ArrayList<>();

        MapContext<LongWritable, Text, Text, EventCountWritable> mapContext =
            (MapContext<LongWritable, Text, Text, EventCountWritable>) Proxy.newProxyInstance(
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
                        EventCountWritable v = (EventCountWritable) args[1];
                        emitted.add(new AbstractMap.SimpleEntry<>(
                            k,
                            new EventCountWritable(v.getViews(), v.getCarts(), v.getRemoves(), v.getPurchases(), v.getMalformed())
                        ));
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedMapper<LongWritable, Text, Text, EventCountWritable>().getMapContext(mapContext);
    }

    private void verifyCounterIncrement(QualityCounter counter, long expected) {
        String key = QualityCounter.GROUP_NAME + ":" + counter.name();
        long actual = counterValues.getOrDefault(key, 0L);
        assertEquals(expected, actual, "Counter mismatch for " + counter.name());
    }

    @Test
    @DisplayName("Valid VIEW record emits date-hour key with views=1 and increments VALID_RECORDS")
    void testValidViewEvent() throws Exception {
        String line = "2019-11-01 00:00:02 UTC,view,1001,2001,electronics.smartphone,acme,199.90,11,sess-A";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertEquals(1, emitted.size());
        assertEquals("2019-11-01 00", emitted.get(0).getKey());
        EventCountWritable val = emitted.get(0).getValue();
        assertEquals(1L, val.getViews());
        assertEquals(0L, val.getCarts());
        assertEquals(0L, val.getMalformed());

        verifyCounterIncrement(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.VALID_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.HEADER_SKIPPED, 0);
    }

    @Test
    @DisplayName("All four event types emit correct counter field")
    void testAllEventTypes() throws Exception {
        String[] eventTypes = {"view", "cart", "remove_from_cart", "purchase"};
        for (String type : eventTypes) {
            String line = "2019-11-01 05:00:00 UTC," + type + ",1001,2001,cat,brand,10.00,1,sess-1";
            mapper.map(new LongWritable(0), new Text(line), context);
        }

        assertEquals(4, emitted.size());
        assertEquals(1L, emitted.get(0).getValue().getViews());
        assertEquals(1L, emitted.get(1).getValue().getCarts());
        assertEquals(1L, emitted.get(2).getValue().getRemoves());
        assertEquals(1L, emitted.get(3).getValue().getPurchases());
    }

    @Test
    @DisplayName("Header line increments TOTAL_RECORDS and HEADER_SKIPPED and emits nothing")
    void testHeaderHandling() throws Exception {
        String header = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session";
        mapper.map(new LongWritable(0), new Text(header), context);

        assertTrue(emitted.isEmpty(), "Header line should not emit any record");
        verifyCounterIncrement(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.HEADER_SKIPPED, 1);
        verifyCounterIncrement(QualityCounter.VALID_RECORDS, 0);
    }

    @Test
    @DisplayName("Attributable malformed record emits date-hour key with malformed=1")
    void testAttributableMalformedRecord() throws Exception {
        // Valid timestamp (2019-11-01 01), invalid non-numeric product_id
        String line = "2019-11-01 01:20:00 UTC,view,abc,2001,x.y,acme,3.00,15,sess-E";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertEquals(1, emitted.size());
        assertEquals("2019-11-01 01", emitted.get(0).getKey());
        assertEquals(1L, emitted.get(0).getValue().getMalformed());
        assertEquals(0L, emitted.get(0).getValue().getTotal());

        verifyCounterIncrement(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.MALFORMED_ID, 1);
        verifyCounterIncrement(QualityCounter.VALID_RECORDS, 0);
    }

    @Test
    @DisplayName("Unattributable malformed record emits INVALID_TIME key with malformed=1")
    void testUnattributableMalformedRecord() throws Exception {
        // Month 13
        String line = "2019-13-01 01:10:00 UTC,view,1001,2001,electronics.smartphone,acme,199.90,14,sess-D";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertEquals(1, emitted.size());
        assertEquals("INVALID_TIME", emitted.get(0).getKey());
        assertEquals(1L, emitted.get(0).getValue().getMalformed());

        verifyCounterIncrement(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.MALFORMED_TIMESTAMP, 1);
    }

    @Test
    @DisplayName("Missing category and brand increment informational counters but emit valid record")
    void testMissingMetadataCounters() throws Exception {
        String line = "2019-11-01 00:59:59 UTC,view,1002,2002,,,5.00,12,sess-B";
        mapper.map(new LongWritable(0), new Text(line), context);

        verifyCounterIncrement(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.VALID_RECORDS, 1);
        verifyCounterIncrement(QualityCounter.MISSING_CATEGORY, 1);
        verifyCounterIncrement(QualityCounter.MISSING_BRAND, 1);
    }
}
