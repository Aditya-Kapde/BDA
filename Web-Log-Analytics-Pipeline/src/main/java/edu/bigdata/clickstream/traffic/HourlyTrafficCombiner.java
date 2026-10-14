package edu.bigdata.clickstream.traffic;

import edu.bigdata.clickstream.writable.EventCountWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * HourlyTrafficCombiner implements associative, in-mapper / intermediate aggregation
 * for Job J1 per SRS §3.3.2 (FR-MR-04) and §3.3.4.
 *
 * Produces identical output regardless of how many times Hadoop invokes it.
 * Does not increment record-level counters to avoid double-counting.
 */
public class HourlyTrafficCombiner extends Reducer<Text, EventCountWritable, Text, EventCountWritable> {

    private final EventCountWritable combinedValue = new EventCountWritable();

    @Override
    protected void reduce(Text key, Iterable<EventCountWritable> values, Context context)
            throws IOException, InterruptedException {
        combinedValue.reset();

        for (EventCountWritable val : values) {
            combinedValue.merge(val);
        }

        context.write(key, combinedValue);
    }
}
