package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.common.TimeUtil;
import edu.bigdata.clickstream.writable.SessionAggWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * SessionFunnelReducer aggregates session-level states and folds them into daily
 * behavioral funnel totals per SRS §3.3.8 (FR-MR-14).
 *
 * Input:
 *   Key:   Text(user_session)
 *   Value: Iterable<SessionAggWritable>
 *
 * Output:
 *   Key:   NullWritable
 *   Value: date \t sessions \t cart \t purchase \t abandoned \t removes
 *
 * Metrics Array definition (long[5]):
 *   [0] sessions: total distinct valid sessions
 *   [1] cart_sessions: sessions with >= 1 cart event (flags & 0x02 != 0)
 *   [2] purchase_sessions: sessions with >= 1 purchase event (flags & 0x04 != 0)
 *   [3] abandoned_sessions: sessions with >= 1 cart and 0 purchase events (isAbandoned())
 *   [4] remove_events_in_cart_sessions: total remove_from_cart events in cart sessions
 *
 * Date Attribution:
 *   A session spanning multiple timestamps belongs strictly to the UTC calendar date
 *   of its earliest event (minEpoch) per SRS §3.3.8 and PRD R7.
 *
 * Memory Invariant:
 *   Folded into an in-memory TreeMap<date, long[5]>. Since batch runs operate on a
 *   single day partition or a bounded backfill window, memory is strictly O(distinct dates) <= 60 dates.
 */
public class SessionFunnelReducer extends Reducer<Text, SessionAggWritable, NullWritable, Text> {

    private final SessionAggWritable accumulator = new SessionAggWritable();
    private final TreeMap<String, long[]> dateMap = new TreeMap<>();
    private final Text outText = new Text();

    public SessionFunnelReducer() {
    }

    Map<String, long[]> getDateMap() {
        return Collections.unmodifiableMap(dateMap);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dateMap.clear();
    }

    @Override
    protected void reduce(Text key, Iterable<SessionAggWritable> values, Context context) throws IOException, InterruptedException {
        accumulator.reset();
        for (SessionAggWritable val : values) {
            accumulator.merge(val);
        }

        if (accumulator.getMinEpoch() <= 0L) {
            return;
        }

        String sessionDate = TimeUtil.epochSecondsToDate(accumulator.getMinEpoch());
        long[] metrics = dateMap.computeIfAbsent(sessionDate, d -> new long[5]);

        // [0] sessions
        metrics[0]++;

        // [1] cart_sessions & [4] remove_events_in_cart_sessions
        if (accumulator.hasCart()) {
            metrics[1]++;
            metrics[4] += accumulator.getRemoveCount();
        }

        // [2] purchase_sessions
        if (accumulator.hasPurchase()) {
            metrics[2]++;
        }

        // [3] abandoned_sessions: cart && !purchase
        if (accumulator.isAbandoned()) {
            metrics[3]++;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, long[]> entry : dateMap.entrySet()) {
            String date = entry.getKey();
            long[] m = entry.getValue();
            outText.set(date + "\t" + m[0] + "\t" + m[1] + "\t" + m[2] + "\t" + m[3] + "\t" + m[4]);
            context.write(NullWritable.get(), outText);
        }
    }
}
