package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.common.*;
import edu.bigdata.clickstream.writable.SessionAggWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * SessionFunnelMapper processes raw clickstream logs and emits session-level
 * behavioral events per SRS §3.3.8 (FR-MR-14).
 *
 * Input:
 *   Raw clickstream CSV partition (e.g. /clickstream/raw/dt=YYYY-MM-DD/events.csv).
 *
 * Output:
 *   Key:   Text(user_session)
 *   Value: SessionAggWritable(minEpoch, flagsMask, removeCount)
 *
 * Rules:
 *   - Parses rows using the normative ClickEventParser.
 *   - Headers and malformed records increment ClickstreamQuality counters and emit nothing.
 *   - Valid records emit:
 *     - minEpoch: UTC epoch seconds
 *     - flags: bit0 for VIEW (0x01), bit1 for CART (0x02), bit2 for PURCHASE (0x04)
 *     - removeCount: 1 if event_type is REMOVE_FROM_CART, else 0
 */
public class SessionFunnelMapper extends Mapper<LongWritable, Text, Text, SessionAggWritable> {

    private final ClickEventParser parser = new ClickEventParser();
    private final ClickEvent event = new ClickEvent();
    private final Text sessionKey = new Text();
    private final SessionAggWritable outVal = new SessionAggWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ClickEventParser.ParseResult res = parser.parse(value.toString(), event);

        // Header detection: increment counters and skip record
        if (res.getStatus() == ClickEventParser.ParseStatus.HEADER) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.TOTAL_RECORDS.name()).increment(1);
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.HEADER_SKIPPED.name()).increment(1);
            return;
        }

        // All non-header records increment TOTAL_RECORDS
        context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.TOTAL_RECORDS.name()).increment(1);

        if (res.getStatus() == ClickEventParser.ParseStatus.VALID) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.VALID_RECORDS.name()).increment(1);
            if (res.isMissingCategory()) {
                context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MISSING_CATEGORY.name()).increment(1);
            }
            if (res.isMissingBrand()) {
                context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MISSING_BRAND.name()).increment(1);
            }

            long epochSec = event.getEpochSeconds();
            EventType type = event.getEventType();
            byte flags = type != null ? type.getFlagBit() : (byte) 0;
            int removeCount = (type == EventType.REMOVE_FROM_CART) ? 1 : 0;

            sessionKey.set(event.getUserSession());
            outVal.set(epochSec, flags, removeCount);
            context.write(sessionKey, outVal);

        } else if (res.getStatus() == ClickEventParser.ParseStatus.MALFORMED) {
            // Increment the specific malformed counter
            if (res.getCounter() != null) {
                context.getCounter(QualityCounter.GROUP_NAME, res.getCounter().name()).increment(1);
            }
        }
    }
}
