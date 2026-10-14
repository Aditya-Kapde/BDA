package edu.bigdata.clickstream.traffic;

import edu.bigdata.clickstream.writable.EventCountWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

/**
 * HourlyTrafficReducer consolidates hourly event counts and formats final output
 * per SRS §3.3.4 (FR-MR-10).
 *
 * Tab-separated output schema:
 *   date \t hour \t total \t views \t carts \t removes \t purchases \t malformed \t error_rate
 *
 * Special key handling:
 *   "INVALID_TIME" -> "NA\tNA\t0\t0\t0\t0\t0\t<n>\t1.000000"
 */
public class HourlyTrafficReducer extends Reducer<Text, EventCountWritable, NullWritable, Text> {

    public static final String UNATTRIBUTABLE_KEY = "INVALID_TIME";

    private final EventCountWritable totalValue = new EventCountWritable();
    private final Text resultLine = new Text();

    @Override
    protected void reduce(Text key, Iterable<EventCountWritable> values, Context context)
            throws IOException, InterruptedException {
        totalValue.reset();

        for (EventCountWritable val : values) {
            totalValue.merge(val);
        }

        String keyStr = key.toString();
        String line;

        if (UNATTRIBUTABLE_KEY.equals(keyStr)) {
            // Unattributable malformed record line per SRS §3.3.4
            line = "NA\tNA\t0\t0\t0\t0\t0\t" + totalValue.getMalformed() + "\t1.000000";
        } else {
            // Key format: "yyyy-MM-dd HH"
            String date;
            int hour;
            if (keyStr.length() >= 13) {
                date = keyStr.substring(0, 10);
                try {
                    hour = Integer.parseInt(keyStr.substring(11).trim());
                } catch (NumberFormatException e) {
                    hour = -1;
                }
            } else {
                date = keyStr;
                hour = -1;
            }

            long total = totalValue.getTotal();
            long views = totalValue.getViews();
            long carts = totalValue.getCarts();
            long removes = totalValue.getRemoves();
            long purchases = totalValue.getPurchases();
            long malformed = totalValue.getMalformed();
            double errorRate = totalValue.getErrorRate();
            String formattedErrorRate = String.format(Locale.ROOT, "%.6f", errorRate);

            line = date + "\t" +
                   hour + "\t" +
                   total + "\t" +
                   views + "\t" +
                   carts + "\t" +
                   removes + "\t" +
                   purchases + "\t" +
                   malformed + "\t" +
                   formattedErrorRate;
        }

        resultLine.set(line);
        context.write(NullWritable.get(), resultLine);
    }
}
