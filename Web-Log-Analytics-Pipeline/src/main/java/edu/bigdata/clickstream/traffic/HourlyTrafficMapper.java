package edu.bigdata.clickstream.traffic;

import edu.bigdata.clickstream.common.ClickEvent;
import edu.bigdata.clickstream.common.ClickEventParser;
import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.EventCountWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * HourlyTrafficMapper implements the mapping stage of Job J1 per SRS §3.3.4 (FR-MR-10).
 *
 * Responsibilities:
 * - Parses input lines using ClickEventParser.
 * - Increments QualityCounter metrics on Hadoop Context.
 * - Emits:
 *     - Valid events: Key "yyyy-MM-dd HH", Value EventCountWritable with appropriate event count = 1.
 *     - Attributable malformed: Key "yyyy-MM-dd HH", Value EventCountWritable with malformed = 1.
 *     - Unattributable malformed: Key "INVALID_TIME", Value EventCountWritable with malformed = 1.
 * - Ignores CSV headers without emitting records.
 */
public class HourlyTrafficMapper extends Mapper<LongWritable, Text, Text, EventCountWritable> {

    public static final String UNATTRIBUTABLE_KEY = "INVALID_TIME";

    private final ClickEventParser parser = new ClickEventParser();
    private final ClickEvent event = new ClickEvent();
    private final Text outputKey = new Text();
    private final EventCountWritable outputValue = new EventCountWritable();

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

            outputKey.set(res.getDateHour());
            outputValue.reset();

            switch (event.getEventType()) {
                case VIEW:
                    outputValue.setViews(1L);
                    break;
                case CART:
                    outputValue.setCarts(1L);
                    break;
                case REMOVE_FROM_CART:
                    outputValue.setRemoves(1L);
                    break;
                case PURCHASE:
                    outputValue.setPurchases(1L);
                    break;
            }

            context.write(outputKey, outputValue);

        } else if (res.getStatus() == ClickEventParser.ParseStatus.MALFORMED) {
            // Increment the specific malformed counter
            if (res.getCounter() != null) {
                context.getCounter(QualityCounter.GROUP_NAME, res.getCounter().name()).increment(1);
            }

            if (res.isAttributable()) {
                outputKey.set(res.getDateHour());
            } else {
                outputKey.set(UNATTRIBUTABLE_KEY);
            }

            outputValue.reset();
            outputValue.setMalformed(1L);

            context.write(outputKey, outputValue);
        }
    }
}
