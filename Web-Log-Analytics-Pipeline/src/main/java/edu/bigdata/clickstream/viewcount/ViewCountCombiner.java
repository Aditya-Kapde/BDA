package edu.bigdata.clickstream.viewcount;

import edu.bigdata.clickstream.writable.ViewCountWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ViewCountCombiner implements map-side associative/commutative aggregation for Job J3 per SRS §3.3.6 (FR-MR-12).
 *
 * Merges:
 * - count: sum of counts
 * - category: lexicographically smallest non-empty category code
 * - brand: lexicographically smallest non-empty brand name
 *
 * Does not modify record-level counters.
 */
public class ViewCountCombiner extends Reducer<Text, ViewCountWritable, Text, ViewCountWritable> {

    private final ViewCountWritable accumulator = new ViewCountWritable();

    @Override
    protected void reduce(Text key, Iterable<ViewCountWritable> values, Context context) throws IOException, InterruptedException {
        accumulator.reset();
        for (ViewCountWritable val : values) {
            accumulator.merge(val);
        }
        context.write(key, accumulator);
    }
}
