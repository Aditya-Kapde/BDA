package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.writable.SessionAggWritable;
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

public class SessionFunnelReducerTest {

    private SessionFunnelReducer reducer;
    private List<String> capturedOutputs;
    private Reducer<Text, SessionAggWritable, NullWritable, Text>.Context testContext;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        reducer = new SessionFunnelReducer();
        capturedOutputs = new ArrayList<>();

        ReduceContext<Text, SessionAggWritable, NullWritable, Text> reduceCtx =
            (ReduceContext<Text, SessionAggWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        capturedOutputs.add(args[1].toString());
                    }
                    return null;
                }
            );

        testContext = new WrappedReducer<Text, SessionAggWritable, NullWritable, Text>().getReducerContext(reduceCtx);
    }

    @Test
    @DisplayName("Empty input produces zero output records")
    void testEmptyInput() throws Exception {
        reducer.setup(testContext);
        reducer.cleanup(testContext);
        assertTrue(capturedOutputs.isEmpty(), "No output should be generated for empty input");
    }

    @Test
    @DisplayName("Session containing only a view produces: sessions=1, cart=0, purchase=0, abandoned=0, removes=0")
    void testSessionViewOnly() throws Exception {
        reducer.setup(testContext);

        // 2019-11-01 00:00:00 UTC = 1572566400L
        List<SessionAggWritable> vals = Collections.singletonList(
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_VIEW, 0)
        );

        reducer.reduce(new Text("sess-view"), vals, testContext);
        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        assertEquals("2019-11-01\t1\t0\t0\t0\t0", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Session containing view followed by cart: sessions=1, cart=1, purchase=0, abandoned=1, removes=0")
    void testSessionViewAndCart() throws Exception {
        reducer.setup(testContext);

        List<SessionAggWritable> vals = Arrays.asList(
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572566450L, SessionAggWritable.FLAG_CART, 0)
        );

        reducer.reduce(new Text("sess-vc"), vals, testContext);
        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        assertEquals("2019-11-01\t1\t1\t0\t1\t0", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Session containing cart followed by purchase: not abandoned")
    void testSessionCartAndPurchase() throws Exception {
        reducer.setup(testContext);

        List<SessionAggWritable> vals = Arrays.asList(
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572566460L, SessionAggWritable.FLAG_PURCHASE, 0)
        );

        reducer.reduce(new Text("sess-cp"), vals, testContext);
        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        // sessions=1, cart=1, purchase=1, abandoned=0, removes=0
        assertEquals("2019-11-01\t1\t1\t1\t0\t0", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Session with cart but no purchase: classified as abandoned")
    void testSessionCartNoPurchaseAbandoned() throws Exception {
        reducer.setup(testContext);

        List<SessionAggWritable> vals = Arrays.asList(
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572566420L, SessionAggWritable.FLAG_CART, 0)
        );

        reducer.reduce(new Text("sess-abandoned"), vals, testContext);
        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        assertEquals("2019-11-01\t1\t1\t0\t1\t0", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Session with cart, purchase, and remove events: purchase prevents abandonment, removes counted")
    void testSessionPurchaseWithRemoves() throws Exception {
        reducer.setup(testContext);

        List<SessionAggWritable> vals = Arrays.asList(
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572566410L, (byte) 0, 2), // 2 removals
            new SessionAggWritable(1572566420L, SessionAggWritable.FLAG_PURCHASE, 0)
        );

        reducer.reduce(new Text("sess-cpr"), vals, testContext);
        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        // sessions=1, cart=1, purchase=1, abandoned=0, removes=2
        assertEquals("2019-11-01\t1\t1\t1\t0\t2", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Repeated view, cart, purchase, and removal events in arbitrary arrival order")
    void testRepeatedAndReorderedEvents() throws Exception {
        // Run forward order
        reducer.setup(testContext);
        List<SessionAggWritable> forward = Arrays.asList(
            new SessionAggWritable(1572566500L, SessionAggWritable.FLAG_PURCHASE, 0),
            new SessionAggWritable(1572566400L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572566410L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572566450L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572566460L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572566470L, (byte) 0, 1),
            new SessionAggWritable(1572566480L, (byte) 0, 2)
        );

        reducer.reduce(new Text("sess-repeat"), forward, testContext);
        reducer.cleanup(testContext);
        String forwardResult = capturedOutputs.get(0);

        // Run reverse order on new reducer instance
        SessionFunnelReducer reverseReducer = new SessionFunnelReducer();
        capturedOutputs.clear();
        reverseReducer.setup(testContext);
        List<SessionAggWritable> reverse = new ArrayList<>(forward);
        Collections.reverse(reverse);

        reverseReducer.reduce(new Text("sess-repeat"), reverse, testContext);
        reverseReducer.cleanup(testContext);
        String reverseResult = capturedOutputs.get(0);

        assertEquals(forwardResult, reverseResult);
        // sessions=1, cart=1, purchase=1, abandoned=0, removes=3
        assertEquals("2019-11-01\t1\t1\t1\t0\t3", forwardResult);
    }

    @Test
    @DisplayName("Midnight crossing session: attributed to earliest event UTC date")
    void testMidnightCrossingSessionAttribution() throws Exception {
        reducer.setup(testContext);

        // Event 1: 2019-11-01 23:59:59 UTC = 1572652799L (Cart)
        // Event 2: 2019-11-02 00:05:00 UTC = 1572653100L (Remove from cart)
        List<SessionAggWritable> vals = Arrays.asList(
            new SessionAggWritable(1572652799L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572653100L, (byte) 0, 1)
        );

        reducer.reduce(new Text("sess-midnight"), vals, testContext);
        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        // Date must strictly be 2019-11-01
        assertEquals("2019-11-01\t1\t1\t0\t1\t1", capturedOutputs.get(0));
    }

    @Test
    @DisplayName("Multiple dates produce deterministic chronological sort order")
    void testMultipleDatesChronologicalOrder() throws Exception {
        reducer.setup(testContext);

        // Day 2 session: 2019-11-02 10:00:00 UTC = 1572688800L
        reducer.reduce(new Text("sess-day2"),
            Collections.singletonList(new SessionAggWritable(1572688800L, SessionAggWritable.FLAG_VIEW, 0)),
            testContext);

        // Day 1 session: 2019-11-01 10:00:00 UTC = 1572602400L
        reducer.reduce(new Text("sess-day1"),
            Collections.singletonList(new SessionAggWritable(1572602400L, SessionAggWritable.FLAG_VIEW, 0)),
            testContext);

        reducer.cleanup(testContext);

        assertEquals(2, capturedOutputs.size());
        assertEquals("2019-11-01\t1\t0\t0\t0\t0", capturedOutputs.get(0));
        assertEquals("2019-11-02\t1\t0\t0\t0\t0", capturedOutputs.get(1));
    }

    @Test
    @DisplayName("Records with minEpoch <= 0 are skipped")
    void testInvalidEpochSkipped() throws Exception {
        reducer.setup(testContext);

        reducer.reduce(new Text("sess-invalid"),
            Collections.singletonList(new SessionAggWritable(0L, SessionAggWritable.FLAG_VIEW, 0)),
            testContext);

        reducer.cleanup(testContext);
        assertTrue(capturedOutputs.isEmpty(), "Zero minEpoch records should not generate daily output");
    }

    @Test
    @DisplayName("Full reproduction of mini_clickstream.csv 6 valid sessions")
    void testMiniClickstreamSessionsFullAggregation() throws Exception {
        reducer.setup(testContext);

        // sess-A: view + cart + purchase (2019-11-01)
        reducer.reduce(new Text("sess-A"), Arrays.asList(
            new SessionAggWritable(1572566402L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572566403L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572566405L, SessionAggWritable.FLAG_PURCHASE, 0)
        ), testContext);

        // sess-B: view only (2019-11-01)
        reducer.reduce(new Text("sess-B"), Collections.singletonList(
            new SessionAggWritable(1572569999L, SessionAggWritable.FLAG_VIEW, 0)
        ), testContext);

        // sess-C: 2 views (2019-11-01)
        reducer.reduce(new Text("sess-C"), Arrays.asList(
            new SessionAggWritable(1572570000L, SessionAggWritable.FLAG_VIEW, 0),
            new SessionAggWritable(1572570005L, SessionAggWritable.FLAG_VIEW, 0)
        ), testContext);

        // sess-Z: cart on 11-01 (23:59:59) + remove on 11-02 (00:00:01)
        reducer.reduce(new Text("sess-Z"), Arrays.asList(
            new SessionAggWritable(1572652799L, SessionAggWritable.FLAG_CART, 0),
            new SessionAggWritable(1572652801L, (byte) 0, 1)
        ), testContext);

        // sess-T1: view only (2019-11-01 02:00:00)
        reducer.reduce(new Text("sess-T1"), Collections.singletonList(
            new SessionAggWritable(1572573600L, SessionAggWritable.FLAG_VIEW, 0)
        ), testContext);

        // sess-T2: view only (2019-11-01 02:05:00)
        reducer.reduce(new Text("sess-T2"), Collections.singletonList(
            new SessionAggWritable(1572573900L, SessionAggWritable.FLAG_VIEW, 0)
        ), testContext);

        reducer.cleanup(testContext);

        assertEquals(1, capturedOutputs.size());
        // 2019-11-01: sessions=6, cart=2, purchase=1, abandoned=1, removes=1
        assertEquals("2019-11-01\t6\t2\t1\t1\t1", capturedOutputs.get(0));
    }
}
