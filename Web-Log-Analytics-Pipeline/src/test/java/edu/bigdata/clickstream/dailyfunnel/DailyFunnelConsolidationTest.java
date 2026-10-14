package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.writable.DailyFunnelWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DailyFunnelConsolidationTest {

    @Test
    @DisplayName("Critical Multi-Partition Consolidation: 8 simulated J5 reducer partitions consolidate into exact daily totals")
    void testMultiPartitionConsolidationAcross8Reducers() throws Exception {
        // 8 simulated J5 reducer partition outputs
        Map<String, List<String>> partitions = new LinkedHashMap<>();
        partitions.put("part-r-00000", Arrays.asList(
            "2019-11-01\t10\t4\t2\t2\t1",
            "2019-11-02\t20\t8\t5\t3\t2"
        ));
        partitions.put("part-r-00001", Collections.singletonList(
            "2019-11-01\t15\t5\t3\t2\t0"
        ));
        partitions.put("part-r-00002", Collections.singletonList(
            "2019-11-03\t30\t12\t7\t5\t4"
        ));
        partitions.put("part-r-00003", Arrays.asList(
            "2019-11-01\t5\t1\t1\t0\t0",
            "2019-11-02\t10\t4\t2\t2\t1"
        ));
        partitions.put("part-r-00004", Collections.singletonList(
            "2019-11-02\t5\t2\t1\t1\t0"
        ));
        partitions.put("part-r-00005", Collections.singletonList(
            "2019-11-01\t20\t8\t4\t4\t3"
        ));
        partitions.put("part-r-00006", Collections.singletonList(
            "2019-11-03\t10\t3\t2\t1\t1"
        ));
        partitions.put("part-r-00007", Arrays.asList(
            "2019-11-01\t10\t2\t2\t0\t0",
            "2019-11-02\t15\t6\t4\t2\t1"
        ));

        // Step 1: Mapping phase across all partitions
        DailyFunnelMapper mapper = new DailyFunnelMapper();
        Map<String, List<DailyFunnelWritable>> mappedByKey = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : partitions.entrySet()) {
            for (String lineStr : entry.getValue()) {
                @SuppressWarnings("unchecked")
                MapContext<LongWritable, Text, Text, DailyFunnelWritable> mapContext =
                    (MapContext<LongWritable, Text, Text, DailyFunnelWritable>) Proxy.newProxyInstance(
                        MapContext.class.getClassLoader(),
                        new Class<?>[]{MapContext.class},
                        (proxy, method, args) -> {
                            if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                                String dateKey = args[0].toString();
                                DailyFunnelWritable w = (DailyFunnelWritable) args[1];
                                mappedByKey.computeIfAbsent(dateKey, k -> new ArrayList<>())
                                    .add(new DailyFunnelWritable(w.getSessions(), w.getCart(), w.getPurchase(), w.getAbandoned(), w.getRemoves()));
                            }
                            return null;
                        }
                    );
                Mapper<LongWritable, Text, Text, DailyFunnelWritable>.Context mCtx =
                    new WrappedMapper<LongWritable, Text, Text, DailyFunnelWritable>().getMapContext(mapContext);

                mapper.map(new LongWritable(0), new Text(lineStr), mCtx);
            }
        }

        // Verify mapped keys
        assertEquals(3, mappedByKey.size());
        assertTrue(mappedByKey.containsKey("2019-11-01"));
        assertTrue(mappedByKey.containsKey("2019-11-02"));
        assertTrue(mappedByKey.containsKey("2019-11-03"));
        assertEquals(5, mappedByKey.get("2019-11-01").size());
        assertEquals(4, mappedByKey.get("2019-11-02").size());
        assertEquals(2, mappedByKey.get("2019-11-03").size());

        // Step 2: Intermediate Combiner phase (simulating per-mapper local combine)
        DailyFunnelCombiner combiner = new DailyFunnelCombiner();
        Map<String, List<DailyFunnelWritable>> combinedByKey = new HashMap<>();

        for (Map.Entry<String, List<DailyFunnelWritable>> entry : mappedByKey.entrySet()) {
            final List<DailyFunnelWritable> combined = new ArrayList<>();
            @SuppressWarnings("unchecked")
            ReduceContext<Text, DailyFunnelWritable, Text, DailyFunnelWritable> redContext =
                (ReduceContext<Text, DailyFunnelWritable, Text, DailyFunnelWritable>) Proxy.newProxyInstance(
                    ReduceContext.class.getClassLoader(),
                    new Class<?>[]{ReduceContext.class},
                    (proxy, method, args) -> {
                        if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                            DailyFunnelWritable w = (DailyFunnelWritable) args[1];
                            combined.add(new DailyFunnelWritable(w.getSessions(), w.getCart(), w.getPurchase(), w.getAbandoned(), w.getRemoves()));
                        }
                        return null;
                    }
                );
            Reducer<Text, DailyFunnelWritable, Text, DailyFunnelWritable>.Context cCtx =
                new WrappedReducer<Text, DailyFunnelWritable, Text, DailyFunnelWritable>().getReducerContext(redContext);

            combiner.reduce(new Text(entry.getKey()), entry.getValue(), cCtx);
            combinedByKey.put(entry.getKey(), combined);
        }

        // Step 3: Final Reducer phase (1 reducer, keys presented in natural chronological order)
        DailyFunnelReducer reducer = new DailyFunnelReducer();
        List<String> finalOutputs = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ReduceContext<Text, DailyFunnelWritable, NullWritable, Text> reduceCtx =
            (ReduceContext<Text, DailyFunnelWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        finalOutputs.add(args[1].toString());
                    }
                    return null;
                }
            );
        Reducer<Text, DailyFunnelWritable, NullWritable, Text>.Context rCtx =
            new WrappedReducer<Text, DailyFunnelWritable, NullWritable, Text>().getReducerContext(reduceCtx);

        // Sort keys naturally (as Hadoop MapReduce shuffle does with 1 reducer)
        List<String> sortedDates = new ArrayList<>(combinedByKey.keySet());
        Collections.sort(sortedDates);

        for (String dateKey : sortedDates) {
            reducer.reduce(new Text(dateKey), combinedByKey.get(dateKey), rCtx);
        }

        // Assertions on final output
        assertEquals(3, finalOutputs.size(), "Exactly 3 daily records must be produced for 3 dates");
        assertEquals("2019-11-01\t60\t20\t12\t8\t4", finalOutputs.get(0));
        assertEquals("2019-11-02\t50\t20\t12\t8\t4", finalOutputs.get(1));
        assertEquals("2019-11-03\t40\t15\t9\t6\t5", finalOutputs.get(2));
    }

    @Test
    @DisplayName("End-to-end consolidation on mini_clickstream.csv J5 expected row")
    void testMiniClickstreamSingleRowConsolidation() throws Exception {
        DailyFunnelReducer reducer = new DailyFunnelReducer();
        List<String> captured = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ReduceContext<Text, DailyFunnelWritable, NullWritable, Text> reduceCtx =
            (ReduceContext<Text, DailyFunnelWritable, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    if ("write".equals(method.getName()) && args != null && args.length >= 2) {
                        captured.add(args[1].toString());
                    }
                    return null;
                }
            );
        Reducer<Text, DailyFunnelWritable, NullWritable, Text>.Context rCtx =
            new WrappedReducer<Text, DailyFunnelWritable, NullWritable, Text>().getReducerContext(reduceCtx);

        // Feed J5 output for mini_clickstream: 2019-11-01\t6\t2\t1\t1\t1
        reducer.reduce(new Text("2019-11-01"), Collections.singletonList(
            new DailyFunnelWritable(6L, 2L, 1L, 1L, 1L)
        ), rCtx);

        assertEquals(1, captured.size());
        assertEquals("2019-11-01\t6\t2\t1\t1\t1", captured.get(0));
    }
}
