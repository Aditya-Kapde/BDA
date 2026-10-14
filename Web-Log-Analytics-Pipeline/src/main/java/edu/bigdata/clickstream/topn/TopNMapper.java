package edu.bigdata.clickstream.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * TopNMapper implements bounded in-mapper Top-N candidate selection per SRS §3.3.7 (FR-MR-13).
 *
 * Input:
 *   Tab-delimited output from Job J3:
 *   - products:   product_id \t category_code \t brand \t views
 *   - categories: category_code \t views
 *
 * Output:
 *   Key: NullWritable
 *   Value: Text (raw original record of each local Top-N survivor)
 *
 * Mathematical Invariant for Bounded Local Pruning:
 *   Let S be the total set of records partitioned across mappers S_1, ..., S_M.
 *   For each mapper split S_i, selecting the best N candidates T_i subset S_i
 *   according to ranking relation > ensures that every pruned element x in (S_i \ T_i)
 *   is strictly dominated by at least N elements in T_i.
 *   Therefore, x can never belong to the global Top-N of S.
 *   Consequently, Top_N(S) = Top_N(Union_{i=1..M} Top_N(S_i)).
 *   Maintaining TreeMap size <= N throughout mapping is strictly safe and optimal.
 */
public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public static final String CONF_TOPN = "clickstream.topn";
    public static final int DEFAULT_TOPN = 10;

    private int topN = DEFAULT_TOPN;
    private final TreeMap<TopNCandidate, Text> map = new TreeMap<>();

    public TopNMapper() {
    }

    // Package-private constructor for unit testing with custom N
    TopNMapper(int topN) {
        this.topN = topN > 0 ? topN : 1;
    }

    void setTopN(int topN) {
        this.topN = topN > 0 ? topN : 1;
    }

    int getTopN() {
        return topN;
    }

    NavigableMap<TopNCandidate, Text> getCandidateMap() {
        return Collections.unmodifiableNavigableMap(map);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context != null && context.getConfiguration() != null) {
            this.topN = context.getConfiguration().getInt(CONF_TOPN, DEFAULT_TOPN);
            if (this.topN < 1) {
                this.topN = 1;
            }
        }
        map.clear();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            return;
        }

        TopNCandidate candidate = TopNCandidate.parse(value.toString());
        if (candidate == null) {
            return;
        }

        map.put(candidate, new Text(candidate.getOriginalLine()));
        if (map.size() > topN) {
            map.pollLastEntry();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text lineText : map.values()) {
            context.write(NullWritable.get(), lineText);
        }
    }
}
