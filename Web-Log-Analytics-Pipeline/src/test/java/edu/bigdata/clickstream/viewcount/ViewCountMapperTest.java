package edu.bigdata.clickstream.viewcount;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.ViewCountWritable;
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

public class ViewCountMapperTest {

    private ViewCountMapper mapper;
    private Mapper<LongWritable, Text, Text, ViewCountWritable>.Context context;
    private Map<String, Long> counterValues;
    private List<Map.Entry<String, ViewCountWritable>> emitted;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new ViewCountMapper();
        counterValues = new HashMap<>();
        emitted = new ArrayList<>();

        MapContext<LongWritable, Text, Text, ViewCountWritable> mapContext =
            (MapContext<LongWritable, Text, Text, ViewCountWritable>) Proxy.newProxyInstance(
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
                        ViewCountWritable v = (ViewCountWritable) args[1];
                        emitted.add(new AbstractMap.SimpleEntry<>(
                            k,
                            new ViewCountWritable(v.getCount(), v.getCategory(), v.getBrand())
                        ));
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedMapper<LongWritable, Text, Text, ViewCountWritable>().getMapContext(mapContext);
    }

    private void verifyCounter(QualityCounter counter, long expected) {
        String key = QualityCounter.GROUP_NAME + ":" + counter.name();
        long actual = counterValues.getOrDefault(key, 0L);
        assertEquals(expected, actual, "Counter mismatch for " + counter.name());
    }

    @Test
    @DisplayName("Valid VIEW event emits both P|product_id and C|category_code")
    void testValidViewWithCategoryAndBrand() throws Exception {
        String line = "2019-11-01 00:00:02 UTC,view,1001,2001,electronics.smartphone,acme,199.90,11,sess-A";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertEquals(2, emitted.size());

        // Product emission
        assertEquals("P|1001", emitted.get(0).getKey());
        ViewCountWritable prodVal = emitted.get(0).getValue();
        assertEquals(1L, prodVal.getCount());
        assertEquals("electronics.smartphone", prodVal.getCategory());
        assertEquals("acme", prodVal.getBrand());

        // Category emission
        assertEquals("C|electronics.smartphone", emitted.get(1).getKey());
        ViewCountWritable catVal = emitted.get(1).getValue();
        assertEquals(1L, catVal.getCount());
        assertEquals("", catVal.getCategory());
        assertEquals("", catVal.getBrand());

        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.VALID_RECORDS, 1);
        verifyCounter(QualityCounter.VIEW_EVENTS, 1);
        verifyCounter(QualityCounter.VIEWS_WITHOUT_CATEGORY, 0);
    }

    @Test
    @DisplayName("Valid VIEW event with empty category emits only P|product_id and increments VIEWS_WITHOUT_CATEGORY")
    void testValidViewMissingCategory() throws Exception {
        String line = "2019-11-01 00:59:59 UTC,view,1002,2002,,,5.00,12,sess-B";
        mapper.map(new LongWritable(0), new Text(line), context);

        assertEquals(1, emitted.size());
        assertEquals("P|1002", emitted.get(0).getKey());
        ViewCountWritable prodVal = emitted.get(0).getValue();
        assertEquals(1L, prodVal.getCount());
        assertEquals("", prodVal.getCategory());
        assertEquals("", prodVal.getBrand());

        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.VALID_RECORDS, 1);
        verifyCounter(QualityCounter.MISSING_CATEGORY, 1);
        verifyCounter(QualityCounter.MISSING_BRAND, 1);
        verifyCounter(QualityCounter.VIEW_EVENTS, 1);
        verifyCounter(QualityCounter.VIEWS_WITHOUT_CATEGORY, 1);
    }

    @Test
    @DisplayName("Non-view events (cart, remove_from_cart, purchase) do not emit anything")
    void testNonViewEventsFilteredOut() throws Exception {
        String[] lines = {
            "2019-11-01 00:00:40 UTC,cart,1001,2001,electronics.smartphone,acme,199.90,11,sess-A",
            "2019-11-01 01:10:00 UTC,purchase,1001,2001,electronics.smartphone,acme,199.90,11,sess-A",
            "2019-11-02 00:00:01 UTC,remove_from_cart,1003,2003,appliances.kitchen,brandx,50.00,20,sess-Z"
        };

        for (String line : lines) {
            mapper.map(new LongWritable(0), new Text(line), context);
        }

        assertTrue(emitted.isEmpty(), "Non-view events must not emit any view records");
        verifyCounter(QualityCounter.TOTAL_RECORDS, 3);
        verifyCounter(QualityCounter.VALID_RECORDS, 3);
        verifyCounter(QualityCounter.VIEW_EVENTS, 0);
    }

    @Test
    @DisplayName("Header row increments HEADER_SKIPPED and emits nothing")
    void testHeaderHandling() throws Exception {
        String header = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session";
        mapper.map(new LongWritable(0), new Text(header), context);

        assertTrue(emitted.isEmpty());
        verifyCounter(QualityCounter.TOTAL_RECORDS, 1);
        verifyCounter(QualityCounter.HEADER_SKIPPED, 1);
        verifyCounter(QualityCounter.VALID_RECORDS, 0);
    }

    @Test
    @DisplayName("Malformed records increment specific counters and emit nothing")
    void testMalformedRecords() throws Exception {
        // Non-numeric product_id
        String lineId = "2019-11-01 01:20:00 UTC,view,abc,2001,x.y,acme,3.00,15,sess-E";
        mapper.map(new LongWritable(0), new Text(lineId), context);

        // Invalid timestamp
        String lineTime = "2019-13-01 01:10:00 UTC,view,1001,2001,electronics.smartphone,acme,199.90,14,sess-D";
        mapper.map(new LongWritable(0), new Text(lineTime), context);

        // Negative price
        String linePrice = "2019-11-01 01:45:00 UTC,view,1001,2001,x.y,acme,-15.00,15,sess-H";
        mapper.map(new LongWritable(0), new Text(linePrice), context);

        assertTrue(emitted.isEmpty());
        verifyCounter(QualityCounter.TOTAL_RECORDS, 3);
        verifyCounter(QualityCounter.MALFORMED_ID, 1);
        verifyCounter(QualityCounter.MALFORMED_TIMESTAMP, 1);
        verifyCounter(QualityCounter.MALFORMED_PRICE, 1);
        verifyCounter(QualityCounter.VIEW_EVENTS, 0);
    }
}
