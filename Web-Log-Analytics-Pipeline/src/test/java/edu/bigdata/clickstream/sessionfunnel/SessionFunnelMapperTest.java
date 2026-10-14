package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.common.TimeUtil;
import edu.bigdata.clickstream.writable.SessionAggWritable;
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

public class SessionFunnelMapperTest {

    private SessionFunnelMapper mapper;
    private Map<String, Long> counterValues;
    private List<String> writtenKeys;
    private List<SessionAggWritable> writtenValues;
    private Mapper<LongWritable, Text, Text, SessionAggWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new SessionFunnelMapper();
        counterValues = new HashMap<>();
        writtenKeys = new ArrayList<>();
        writtenValues = new ArrayList<>();

        MapContext<LongWritable, Text, Text, SessionAggWritable> mapContext =
            (MapContext<LongWritable, Text, Text, SessionAggWritable>) Proxy.newProxyInstance(
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
                        SessionAggWritable copy = new SessionAggWritable();
                        SessionAggWritable src = (SessionAggWritable) args[1];
                        copy.set(src.getMinEpoch(), src.getFlagsMask(), src.getRemoveCount());
                        writtenValues.add(copy);
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedMapper<LongWritable, Text, Text, SessionAggWritable>().getMapContext(mapContext);
    }

    @Test
    @DisplayName("Valid VIEW event emits flag 0x01 and increments VALID_RECORDS")
    void testValidViewEvent() throws Exception {
        Text line = new Text("2019-11-01 00:00:02 UTC,view,1001,2001,electronics.smartphone,acme,199.90,11,sess-A");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        assertEquals("sess-A", writtenKeys.get(0));

        SessionAggWritable val = writtenValues.get(0);
        assertEquals(TimeUtil.toEpochSeconds("2019-11-01 00:00:02 UTC"), val.getMinEpoch());
        assertEquals(SessionAggWritable.FLAG_VIEW, val.getFlagsMask());
        assertTrue(val.hasView());
        assertFalse(val.hasCart());
        assertFalse(val.hasPurchase());
        assertEquals(0, val.getRemoveCount());

        assertEquals(1L, counterValues.get(QualityCounter.TOTAL_RECORDS.name()));
        assertEquals(1L, counterValues.get(QualityCounter.VALID_RECORDS.name()));
    }

    @Test
    @DisplayName("Valid CART event emits flag 0x02")
    void testValidCartEvent() throws Exception {
        Text line = new Text("2019-11-01 00:00:40 UTC,cart,1001,2001,electronics.smartphone,acme,199.90,11,sess-A");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        assertEquals("sess-A", writtenKeys.get(0));

        SessionAggWritable val = writtenValues.get(0);
        assertEquals(SessionAggWritable.FLAG_CART, val.getFlagsMask());
        assertTrue(val.hasCart());
        assertFalse(val.hasPurchase());
        assertEquals(0, val.getRemoveCount());
    }

    @Test
    @DisplayName("Valid PURCHASE event emits flag 0x04")
    void testValidPurchaseEvent() throws Exception {
        Text line = new Text("2019-11-01 01:10:00 UTC,purchase,1001,2001,electronics.smartphone,acme,199.90,11,sess-A");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        assertEquals("sess-A", writtenKeys.get(0));

        SessionAggWritable val = writtenValues.get(0);
        assertEquals(SessionAggWritable.FLAG_PURCHASE, val.getFlagsMask());
        assertTrue(val.hasPurchase());
        assertEquals(0, val.getRemoveCount());
    }

    @Test
    @DisplayName("Valid REMOVE_FROM_CART event emits removeCount=1 and flags=0")
    void testValidRemoveEvent() throws Exception {
        Text line = new Text("2019-11-02 00:00:01 UTC,remove_from_cart,1003,2003,appliances.kitchen,brandx,50.00,20,sess-Z");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        assertEquals("sess-Z", writtenKeys.get(0));

        SessionAggWritable val = writtenValues.get(0);
        assertEquals(0, val.getFlagsMask());
        assertEquals(1, val.getRemoveCount());
    }

    @Test
    @DisplayName("Header row increments HEADER_SKIPPED and emits nothing")
    void testHeaderRowSkipped() throws Exception {
        Text line = new Text("event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session");
        mapper.map(new LongWritable(0), line, context);

        assertTrue(writtenKeys.isEmpty());
        assertEquals(1L, counterValues.get(QualityCounter.TOTAL_RECORDS.name()));
        assertEquals(1L, counterValues.get(QualityCounter.HEADER_SKIPPED.name()));
    }

    @Test
    @DisplayName("Malformed rows (missing session, bad timestamp, bad column count) emit nothing and update counters")
    void testMalformedRows() throws Exception {
        LongWritable key = new LongWritable(0);

        // Missing session
        mapper.map(key, new Text("2019-11-01 01:50:00 UTC,view,1001,2001,x.y,acme,15.00,15,"), context);
        assertEquals(1L, counterValues.get(QualityCounter.MALFORMED_SESSION.name()));

        // Invalid timestamp
        mapper.map(key, new Text("2019-13-01 01:10:00 UTC,view,1001,2001,x.y,acme,199.90,14,sess-D"), context);
        assertEquals(1L, counterValues.get(QualityCounter.MALFORMED_TIMESTAMP.name()));

        // Invalid column count
        mapper.map(key, new Text("2019-11-01 01:30:00 UTC,view,1001,2001"), context);
        assertEquals(1L, counterValues.get(QualityCounter.MALFORMED_COLUMN_COUNT.name()));

        // Invalid ID
        mapper.map(key, new Text("2019-11-01 01:20:00 UTC,view,abc,2001,x.y,acme,3.00,15,sess-E"), context);
        assertEquals(1L, counterValues.get(QualityCounter.MALFORMED_ID.name()));

        // Negative price
        mapper.map(key, new Text("2019-11-01 01:45:00 UTC,view,1001,2001,x.y,acme,-15.00,15,sess-H"), context);
        assertEquals(1L, counterValues.get(QualityCounter.MALFORMED_PRICE.name()));

        assertTrue(writtenKeys.isEmpty(), "No malformed records should be emitted to session reducer");
    }

    @Test
    @DisplayName("Valid records with missing category or brand still emit session record")
    void testMissingCategoryAndBrand() throws Exception {
        Text line = new Text("2019-11-01 00:59:59 UTC,view,1002,2002,,,5.00,12,sess-B");
        mapper.map(new LongWritable(0), line, context);

        assertEquals(1, writtenKeys.size());
        assertEquals("sess-B", writtenKeys.get(0));
        assertEquals(1L, counterValues.get(QualityCounter.MISSING_CATEGORY.name()));
        assertEquals(1L, counterValues.get(QualityCounter.MISSING_BRAND.name()));
        assertEquals(1L, counterValues.get(QualityCounter.VALID_RECORDS.name()));
    }

    @Test
    @DisplayName("Multiple distinct sessions belonging to the same user are keyed separately by user_session")
    void testMultipleSessionsSameUser() throws Exception {
        LongWritable key = new LongWritable(0);
        mapper.map(key, new Text("2019-11-01 00:00:02 UTC,view,1001,2001,electronics.smartphone,acme,199.90,11,sess-A1"), context);
        mapper.map(key, new Text("2019-11-01 00:00:10 UTC,view,1002,2001,electronics.smartphone,acme,199.90,11,sess-A2"), context);

        assertEquals(2, writtenKeys.size());
        assertEquals("sess-A1", writtenKeys.get(0));
        assertEquals("sess-A2", writtenKeys.get(1));
    }
}
