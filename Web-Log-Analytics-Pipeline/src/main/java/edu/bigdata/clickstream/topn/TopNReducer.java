package edu.bigdata.clickstream.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * TopNReducer merges candidate records from all mappers and outputs the global
 * ranked Top-N list per SRS §3.3.7 (FR-MR-13).
 *
 * Input:
 *   Key: NullWritable
 *   Values: Iterable of candidate lines emitted by TopNMappers
 *
 * Output:
 *   Key: NullWritable
 *   Value: rank \t <original columns> (for ranks 1..N)
 *
 * Execution constraints:
 *   - Strictly 1 reducer task per job (job.setNumReduceTasks(1)).
 *   - Maintains a bounded TreeMap of size at most N, ensuring memory remains O(N)
 *     even when consolidating candidate pools from thousands of mappers.
 *   - Emits deterministically formatted records in strict rank order (1-indexed).
 */
public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    public static final String CONF_TOPN = "clickstream.topn";
    public static final int DEFAULT_TOPN = 10;

    private int topN = DEFAULT_TOPN;
    private final TreeMap<TopNCandidate, Text> reducerMap = new TreeMap<>();
    private final Text outText = new Text();

    public TopNReducer() {
    }

    // Package-private constructor for unit testing with custom N
    TopNReducer(int topN) {
        this.topN = topN > 0 ? topN : 1;
    }

    void setTopN(int topN) {
        this.topN = topN > 0 ? topN : 1;
    }

    int getTopN() {
        return topN;
    }

    NavigableMap<TopNCandidate, Text> getReducerMap() {
        return Collections.unmodifiableNavigableMap(reducerMap);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context != null && context.getConfiguration() != null) {
            this.topN = context.getConfiguration().getInt(CONF_TOPN, DEFAULT_TOPN);
            if (this.topN < 1) {
                this.topN = 1;
            }
        }
        reducerMap.clear();
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (values == null) {
            return;
        }

        for (Text val : values) {
            if (val == null) {
                continue;
            }
            TopNCandidate candidate = TopNCandidate.parse(val.toString());
            if (candidate == null) {
                continue;
            }

            reducerMap.put(candidate, new Text(candidate.getOriginalLine()));
            if (reducerMap.size() > topN) {
                reducerMap.pollLastEntry();
            }
        }

        int rank = 1;
        for (Text lineText : reducerMap.values()) {
            outText.set(rank + "\t" + lineText.toString());
            context.write(NullWritable.get(), outText);
            rank++;
        }
    }
}
