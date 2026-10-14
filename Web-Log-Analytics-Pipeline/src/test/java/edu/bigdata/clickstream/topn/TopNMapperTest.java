package edu.bigdata.clickstream.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TopNMapperTest {

    private TopNMapper mapper;
    private List<String> emittedLines;
    private Mapper<LongWritable, Text, NullWritable, Text>.Context context;
    private Configuration conf;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new TopNMapper();
        emittedLines = new ArrayList<>();
        conf = new Configuration();
        conf.setInt(TopNMapper.CONF_TOPN, 5); // N = 5

        MapContext<LongWritable, Text, NullWritable, Text> mapContext =
            (MapContext<LongWritable, Text, NullWritable, Text>) Proxy.newProxyInstance(
                MapContext.class.getClassLoader(),
                new Class<?>[]{MapContext.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getConfiguration".equals(name)) {
                        return conf;
                    }
                    if ("write".equals(name) && args != null && args.length >= 2) {
                        emittedLines.add(args[1].toString());
                        return null;
                    }
                    return null;
                }
            );

        context = new WrappedMapper<LongWritable, Text, NullWritable, Text>().getMapContext(mapContext);
    }

    @Test
    @DisplayName("In-mapper TreeMap size is strictly bounded by N during mapping")
    void testBoundedStateNeverExceedsCapacity() throws Exception {
        mapper.setup(context);
        assertEquals(5, mapper.getTopN());

        LongWritable key = new LongWritable(0);
        Text val = new Text();

        // Feed 100 items with increasing views
        for (int i = 1; i <= 100; i++) {
            val.set("prod-" + i + "\tcat\tbrand\t" + i);
            mapper.map(key, val, context);

            // Bounded invariant: internal map size must never exceed N (5)
            assertTrue(mapper.getCandidateMap().size() <= 5, "Internal map size must never exceed N");
        }

        assertEquals(5, mapper.getCandidateMap().size());

        // Emit survivors in cleanup
        mapper.cleanup(context);
        assertEquals(5, emittedLines.size());

        // Top 5 views must be 100, 99, 98, 97, 96 in descending order
        assertTrue(emittedLines.get(0).endsWith("\t100"));
        assertTrue(emittedLines.get(1).endsWith("\t99"));
        assertTrue(emittedLines.get(2).endsWith("\t98"));
        assertTrue(emittedLines.get(3).endsWith("\t97"));
        assertTrue(emittedLines.get(4).endsWith("\t96"));
    }

    @Test
    @DisplayName("Local pruning correctly handles random insertion order and tie-breaking")
    void testLocalPruningWithTies() throws Exception {
        mapper.setup(context);

        LongWritable key = new LongWritable(0);
        mapper.map(key, new Text("1005\tcat\tbrand\t50"), context);
        mapper.map(key, new Text("1002\tcat\tbrand\t100"), context);
        mapper.map(key, new Text("1001\tcat\tbrand\t100"), context); // tie with 1002, 1001 < 1002
        mapper.map(key, new Text("1004\tcat\tbrand\t20"), context);
        mapper.map(key, new Text("1003\tcat\tbrand\t80"), context);
        mapper.map(key, new Text("1006\tcat\tbrand\t10"), context); // 6th item, should be pruned

        mapper.cleanup(context);

        // Expected Top 5:
        // Rank 1: 1001 (100)
        // Rank 2: 1002 (100)
        // Rank 3: 1003 (80)
        // Rank 4: 1005 (50)
        // Rank 5: 1004 (20)
        assertEquals(5, emittedLines.size());
        assertTrue(emittedLines.get(0).startsWith("1001\t"));
        assertTrue(emittedLines.get(1).startsWith("1002\t"));
        assertTrue(emittedLines.get(2).startsWith("1003\t"));
        assertTrue(emittedLines.get(3).startsWith("1005\t"));
        assertTrue(emittedLines.get(4).startsWith("1004\t"));
    }

    @Test
    @DisplayName("Malformed, blank, and comment lines are safely skipped")
    void testMalformedLinesSkipped() throws Exception {
        mapper.setup(context);

        LongWritable key = new LongWritable(0);
        mapper.map(key, null, context);
        mapper.map(key, new Text(""), context);
        mapper.map(key, new Text("   "), context);
        mapper.map(key, new Text("# header/comment"), context);
        mapper.map(key, new Text("single_column"), context);
        mapper.map(key, new Text("1001\tinvalid_number"), context);
        mapper.map(key, new Text("1001\tcat\tbrand\t25"), context); // 1 valid

        mapper.cleanup(context);
        assertEquals(1, emittedLines.size());
        assertEquals("1001\tcat\tbrand\t25", emittedLines.get(0));
    }

    @Test
    @DisplayName("Input with fewer than N candidates emits all candidates in rank order")
    void testFewerThanNCandidates() throws Exception {
        conf.setInt(TopNMapper.CONF_TOPN, 10);
        mapper.setup(context);

        LongWritable key = new LongWritable(0);
        mapper.map(key, new Text("electronics.smartphone\t5"), context);
        mapper.map(key, new Text("appliances.kitchen\t2"), context);

        mapper.cleanup(context);
        assertEquals(2, emittedLines.size());
        assertEquals("electronics.smartphone\t5", emittedLines.get(0));
        assertEquals("appliances.kitchen\t2", emittedLines.get(1));
    }
}
