package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.writable.DailyFunnelWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * DailyFunnelCombiner performs associative and commutative pre-aggregation of
 * partial DailyFunnelWritable values for each date per SRS §3.3.9 (FR-MR-15).
 *
 * Invariants:
 * - Associative: (A + B) + C == A + (B + C)
 * - Commutative: A + B == B + A
 * - Idempotent across multiple combiner passes
 * - Does not touch record-level quality counters
 */
public class DailyFunnelCombiner extends Reducer<Text, DailyFunnelWritable, Text, DailyFunnelWritable> {

    private final DailyFunnelWritable accumulator = new DailyFunnelWritable();

    @Override
    protected void reduce(Text key, Iterable<DailyFunnelWritable> values, Context context) throws IOException, InterruptedException {
        accumulator.reset();
        for (DailyFunnelWritable val : values) {
            accumulator.merge(val);
        }
        context.write(key, accumulator);
    }
}
