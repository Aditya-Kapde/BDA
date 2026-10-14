package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.writable.SessionAggWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * SessionFunnelCombiner performs associative and commutative pre-aggregation of
 * SessionAggWritable instances for each user_session per SRS §3.3.2 (FR-MR-04) and §3.3.8.
 *
 * Combiner logic:
 *   - minEpoch: min(minEpoch) (ignoring zero sentinels)
 *   - flagsMask: bitwise OR
 *   - removeCount: field-wise sum
 *
 * Invariants:
 *   - Associative: (A merge B) merge C == A merge (B merge C)
 *   - Commutative: A merge B == B merge A
 *   - Does not increment record-level quality counters.
 *   - Safe across 0, 1, or multiple chunked combiner passes.
 */
public class SessionFunnelCombiner extends Reducer<Text, SessionAggWritable, Text, SessionAggWritable> {

    private final SessionAggWritable accumulator = new SessionAggWritable();

    @Override
    protected void reduce(Text key, Iterable<SessionAggWritable> values, Context context) throws IOException, InterruptedException {
        accumulator.reset();
        for (SessionAggWritable val : values) {
            accumulator.merge(val);
        }
        context.write(key, accumulator);
    }
}
