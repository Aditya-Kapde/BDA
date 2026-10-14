package edu.bigdata.clickstream.viewcount;

import edu.bigdata.clickstream.common.ClickEvent;
import edu.bigdata.clickstream.common.ClickEventParser;
import edu.bigdata.clickstream.common.EventType;
import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.writable.ViewCountWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ViewCountMapper implements the mapping stage of Job J3 (ViewCount) per SRS §3.3.6 (FR-MR-12).
 *
 * Consumes raw clickstream CSV records and processes only valid VIEW events:
 * - Emits ("P|" + product_id) -> ViewCountWritable(1, category_code, brand)
 * - If category_code is non-empty, emits ("C|" + category_code) -> ViewCountWritable(1, "", "")
 *
 * Increments QualityCounter metrics:
 * - ClickstreamQuality: TOTAL_RECORDS, VALID_RECORDS, HEADER_SKIPPED, MALFORMED_*, MISSING_*
 * - Custom counters: VIEW_EVENTS and VIEWS_WITHOUT_CATEGORY
 */
public class ViewCountMapper extends Mapper<LongWritable, Text, Text, ViewCountWritable> {

    public static final String PRODUCT_PREFIX = "P|";
    public static final String CATEGORY_PREFIX = "C|";

    private final ClickEventParser parser = new ClickEventParser();
    private final ClickEvent event = new ClickEvent();
    private final Text outKey = new Text();
    private final ViewCountWritable outVal = new ViewCountWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ClickEventParser.ParseResult res = parser.parse(value.toString(), event);

        // Header handling
        if (res.getStatus() == ClickEventParser.ParseStatus.HEADER) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.TOTAL_RECORDS.name()).increment(1);
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.HEADER_SKIPPED.name()).increment(1);
            return;
        }

        // All non-header records increment TOTAL_RECORDS
        context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.TOTAL_RECORDS.name()).increment(1);

        // Malformed records increment their specific counter and are skipped
        if (res.getStatus() == ClickEventParser.ParseStatus.MALFORMED) {
            if (res.getCounter() != null) {
                context.getCounter(QualityCounter.GROUP_NAME, res.getCounter().name()).increment(1);
            }
            return;
        }

        // VALID record handling
        context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.VALID_RECORDS.name()).increment(1);
        if (res.isMissingCategory()) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MISSING_CATEGORY.name()).increment(1);
        }
        if (res.isMissingBrand()) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MISSING_BRAND.name()).increment(1);
        }

        // Filter: Job J3 only processes valid VIEW events
        if (event.getEventType() != EventType.VIEW) {
            return;
        }

        context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.VIEW_EVENTS.name()).increment(1);

        String categoryCode = event.getCategoryCode();
        String brand = event.getBrand();

        if (categoryCode.isEmpty()) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.VIEWS_WITHOUT_CATEGORY.name()).increment(1);
        }

        // 1. Emit Product view count: ("P|" + product_id) -> ViewCountWritable(1, category_code, brand)
        outKey.set(PRODUCT_PREFIX + event.getProductId());
        outVal.set(1L, categoryCode, brand);
        context.write(outKey, outVal);

        // 2. If category_code is non-empty, emit Category view count: ("C|" + category_code) -> ViewCountWritable(1, "", "")
        if (!categoryCode.isEmpty()) {
            outKey.set(CATEGORY_PREFIX + categoryCode);
            outVal.set(1L, "", "");
            context.write(outKey, outVal);
        }
    }
}
