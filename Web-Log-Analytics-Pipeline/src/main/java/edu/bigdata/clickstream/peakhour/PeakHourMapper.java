package edu.bigdata.clickstream.peakhour;

import edu.bigdata.clickstream.common.QualityCounter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * PeakHourMapper implements the mapping stage of Job J2 per SRS §3.3.5 (FR-MR-11).
 *
 * Consumes J1 output (tab-separated):
 *   date \t hour \t total \t views \t carts \t removes \t purchases \t malformed \t error_rate
 *
 * Responsibilities:
 * - Skips NA / INVALID_TIME records (e.g. date="NA" or hour="NA").
 * - Parses date, hour (0..23), and total event count.
 * - Emits two key-value pairs for each valid hourly record:
 *     1) ("D|" + date) -> ("hour:total")
 *     2) ("H|" + formattedHour) -> ("date:total")
 */
public class PeakHourMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static final String DATE_PREFIX = "D|";
    public static final String HOUR_PREFIX = "H|";
    public static final String NA_VALUE = "NA";

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.TOTAL_RECORDS.name()).increment(1);

        String[] fields = line.split("\t", -1);
        if (fields.length < 3) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MALFORMED_COLUMN_COUNT.name()).increment(1);
            return;
        }

        String date = fields[0].trim();
        String hourStr = fields[1].trim();
        String totalStr = fields[2].trim();

        // Skip unattributable INVALID_TIME rows
        if (NA_VALUE.equalsIgnoreCase(date) || NA_VALUE.equalsIgnoreCase(hourStr)) {
            return;
        }

        int hour;
        long total;
        try {
            hour = Integer.parseInt(hourStr);
            if (hour < 0 || hour > 23) {
                context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MALFORMED_TIMESTAMP.name()).increment(1);
                return;
            }
        } catch (NumberFormatException e) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MALFORMED_TIMESTAMP.name()).increment(1);
            return;
        }

        try {
            total = Long.parseLong(totalStr);
            if (total < 0) {
                context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MALFORMED_ID.name()).increment(1);
                return;
            }
        } catch (NumberFormatException e) {
            context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.MALFORMED_ID.name()).increment(1);
            return;
        }

        context.getCounter(QualityCounter.GROUP_NAME, QualityCounter.VALID_RECORDS.name()).increment(1);

        // 1. Emit for Daily Peak Hour aggregation: ("D|" + date) -> ("hour:total")
        outKey.set(DATE_PREFIX + date);
        outVal.set(hour + ":" + total);
        context.write(outKey, outVal);

        // 2. Emit for Hour-of-Day Profile aggregation: ("H|" + formattedHour) -> ("date:total")
        // Use 2-digit zero-padded hour for deterministic natural sorting in Hadoop shuffle
        outKey.set(String.format("%s%02d", HOUR_PREFIX, hour));
        outVal.set(date + ":" + total);
        context.write(outKey, outVal);
    }
}
