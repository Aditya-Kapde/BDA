package edu.bigdata.clickstream.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Critical Distributed Correctness Test per Specification:
 *
 * Verifies that the bounded local Top-N candidate selection across multiple
 * simulated mapper splits, followed by global single-reducer reduction, produces
 * results mathematically and logically identical to an unconstrained global full sort.
 *
 * Includes adversarial tie-breaking scenarios where multiple candidates have identical
 * counts near the Top-N cutoff boundary and are distributed across different partitions.
 */
public class TopNDistributedRankingTest {

    private static class RawRecord {
        final long views;
        final String key;
        final String rawLine;

        RawRecord(long views, String key) {
            this.views = views;
            this.key = key;
            this.rawLine = key + "\tcategory\tbrand\t" + views;
        }
    }

    @Test
    @DisplayName("Critical Distributed Test: Multi-partition bounded mapping matches global full sort")
    void testDistributedLocalPruningMatchesGlobalFullSort() throws Exception {
        int N = 10;
        int NUM_MAPPERS = 5;

        // 1. Build an adversarial dataset of 60 records
        List<RawRecord> allRecords = new ArrayList<>();

        // High views (uncontested top 5)
        allRecords.add(new RawRecord(1000, "101"));
        allRecords.add(new RawRecord(900, "102"));
        allRecords.add(new RawRecord(850, "103"));
        allRecords.add(new RawRecord(800, "104"));
        allRecords.add(new RawRecord(750, "105"));

        // Boundary Cluster: 10 items all tied at 500 views
        // IDs: "201", "202", ..., "210"
        // Under key ascending tie-break:
        // "201", "202", "203", "204", "205" should take ranks 6, 7, 8, 9, 10!
        // "206", "207", "208", "209", "210" should be pruned!
        for (int i = 1; i <= 10; i++) {
            allRecords.add(new RawRecord(500, String.valueOf(200 + i)));
        }

        // Sub-boundary items: views 499, 450, 300, etc.
        for (int i = 1; i <= 45; i++) {
            allRecords.add(new RawRecord(100 + i, String.valueOf(300 + i)));
        }

        assertEquals(60, allRecords.size());

        // 2. Independent Reference Calculation (Full Sort)
        List<RawRecord> fullSortList = new ArrayList<>(allRecords);
        fullSortList.sort((a, b) -> {
            if (a.views != b.views) {
                return Long.compare(b.views, a.views); // views descending
            }
            return TopNCandidate.compareKeys(a.key, b.key); // key ascending
        });

        List<String> expectedGlobalTopN = new ArrayList<>();
        for (int r = 1; r <= N; r++) {
            RawRecord rec = fullSortList.get(r - 1);
            expectedGlobalTopN.add(r + "\t" + rec.rawLine);
        }

        // 3. Simulated Distributed MapReduce Execution
        // Shuffle records across NUM_MAPPERS partitions adversarially
        List<List<RawRecord>> partitions = new ArrayList<>();
        for (int i = 0; i < NUM_MAPPERS; i++) {
            partitions.add(new ArrayList<>());
        }
        for (int i = 0; i < allRecords.size(); i++) {
            partitions.get(i % NUM_MAPPERS).add(allRecords.get(i));
        }

        Configuration conf = new Configuration();
        conf.setInt(TopNJob.CONF_TOPN, N);

        List<String> intermediateCollector = new ArrayList<>();

        // Run each mapper partition
        for (int m = 0; m < NUM_MAPPERS; m++) {
            TopNMapper mapper = new TopNMapper();

            MapContext<LongWritable, Text, NullWritable, Text> mapContext =
                (MapContext<LongWritable, Text, NullWritable, Text>) Proxy.newProxyInstance(
                    MapContext.class.getClassLoader(),
                    new Class<?>[]{MapContext.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("getConfiguration".equals(name)) return conf;
                        if ("write".equals(name) && args != null && args.length >= 2) {
                            intermediateCollector.add(args[1].toString());
                            return null;
                        }
                        return null;
                    }
                );

            mapper.setup(new WrappedMapper<LongWritable, Text, NullWritable, Text>().getMapContext(mapContext));

            LongWritable dummyKey = new LongWritable(0);
            for (RawRecord rec : partitions.get(m)) {
                mapper.map(dummyKey, new Text(rec.rawLine), new WrappedMapper<LongWritable, Text, NullWritable, Text>().getMapContext(mapContext));
            }

            mapper.cleanup(new WrappedMapper<LongWritable, Text, NullWritable, Text>().getMapContext(mapContext));
        }

        // Verify that intermediate collector received at most NUM_MAPPERS * N records
        assertTrue(intermediateCollector.size() <= NUM_MAPPERS * N,
            "Intermediate pool must be bounded by M * N");

        // 4. Run Single Reducer
        TopNReducer reducer = new TopNReducer();
        List<String> actualGlobalTopN = new ArrayList<>();

        ReduceContext<NullWritable, Text, NullWritable, Text> reduceContext =
            (ReduceContext<NullWritable, Text, NullWritable, Text>) Proxy.newProxyInstance(
                ReduceContext.class.getClassLoader(),
                new Class<?>[]{ReduceContext.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getConfiguration".equals(name)) return conf;
                    if ("write".equals(name) && args != null && args.length >= 2) {
                        actualGlobalTopN.add(args[1].toString());
                        return null;
                    }
                    return null;
                }
            );

        reducer.setup(new WrappedReducer<NullWritable, Text, NullWritable, Text>().getReducerContext(reduceContext));

        List<Text> reducerInputs = new ArrayList<>();
        for (String s : intermediateCollector) {
            reducerInputs.add(new Text(s));
        }

        reducer.reduce(NullWritable.get(), reducerInputs, new WrappedReducer<NullWritable, Text, NullWritable, Text>().getReducerContext(reduceContext));

        // 5. Assert 100% equivalence between distributed local-pruning and full sort
        assertEquals(expectedGlobalTopN.size(), actualGlobalTopN.size());
        for (int i = 0; i < expectedGlobalTopN.size(); i++) {
            assertEquals(expectedGlobalTopN.get(i), actualGlobalTopN.get(i),
                "Discrepancy at rank " + (i + 1));
        }
    }
}
