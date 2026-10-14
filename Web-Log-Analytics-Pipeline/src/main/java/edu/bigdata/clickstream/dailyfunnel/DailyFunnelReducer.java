package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.DailyFunnelWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * DailyFunnelReducer consolidates partial daily funnel records across all J5 reducers
 * into a single final daily record per date per SRS §3.3.9 (FR-MR-15).
 *
 * Input:
 *   Key:   Text(date)
 *   Value: Iterable<DailyFunnelWritable>
 *
 * Output:
 *   Key:   NullWritable
 *   Value: date \t sessions \t cart_sessions \t purchase_sessions \t abandoned_sessions \t remove_events
 */
public class DailyFunnelReducer extends Reducer<Text, DailyFunnelWritable, NullWritable, Text> {

    private final DailyFunnelWritable accumulator = new DailyFunnelWritable();
    private final Text outText = new Text();

    private void increment(Context context, QualityCounter counter) {
        if (context != null) {
            Counter c = context.getCounter(QualityCounter.GROUP_NAME, counter.name());
            if (c != null) {
                c.increment(1);
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<DailyFunnelWritable> values, Context context) throws IOException, InterruptedException {
        accumulator.reset();
        for (DailyFunnelWritable val : values) {
            accumulator.merge(val);
        }

        outText.set(key.toString() + "\t" + accumulator.getSessions() + "\t" + accumulator.getCart()
                + "\t" + accumulator.getPurchase() + "\t" + accumulator.getAbandoned() + "\t" + accumulator.getRemoves());
        context.write(NullWritable.get(), outText);

        increment(context, QualityCounter.DATES_EMITTED);
    }
}
