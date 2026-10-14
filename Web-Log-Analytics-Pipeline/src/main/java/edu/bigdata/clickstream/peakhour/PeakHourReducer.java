package edu.bigdata.clickstream.peakhour;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * PeakHourReducer implements the reduction stage of Job J2 per SRS §3.3.5 (FR-MR-11).
 *
 * Responsibilities:
 * - Uses MultipleOutputs to produce two named outputs:
 *     1) "peakday": date \t peak_hour \t peak_total \t day_total
 *        (max total event count, tie-breaker: smallest hour, day_total = sum of all hour totals)
 *     2) "hourofday": hour \t total_events \t days_observed
 *        (total events across all observed days, days_observed = count of dates with traffic in that hour)
 * - Guarantees O(1) streaming accumulator per key.
 * - Deterministic output formatting and tie-breaking.
 */
public class PeakHourReducer extends Reducer<Text, Text, NullWritable, Text> {

    public static final String OUTPUT_PEAKDAY = "peakday";
    public static final String OUTPUT_HOUROFDAY = "hourofday";

    /**
     * OutputCollector interface decouples MultipleOutputs I/O from core algorithmic reduction,
     * enabling fully deterministic, zero-overhead unit testing without ByteBuddy/Mockito JVM conflicts.
     */
    public interface OutputCollector {
        void write(String namedOutput, Text value) throws IOException, InterruptedException;
        void close() throws IOException, InterruptedException;
    }

    private MultipleOutputs<NullWritable, Text> mos;
    private OutputCollector collector;
    private final Text outText = new Text();

    public PeakHourReducer() {
    }

    // Package-private constructor for injecting test collector
    PeakHourReducer(OutputCollector collector) {
        this.collector = collector;
    }

    void setOutputCollector(OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (collector == null) {
            mos = new MultipleOutputs<>(context);
            collector = new OutputCollector() {
                @Override
                public void write(String namedOutput, Text value) throws IOException, InterruptedException {
                    mos.write(namedOutput, NullWritable.get(), value);
                }

                @Override
                public void close() throws IOException, InterruptedException {
                    mos.close();
                }
            };
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String keyStr = key.toString();

        if (keyStr.startsWith(PeakHourMapper.DATE_PREFIX)) {
            // Daily Peak Hour calculation: key "D|yyyy-MM-dd", value "hour:total"
            String date = keyStr.substring(PeakHourMapper.DATE_PREFIX.length());

            long maxTotal = -1L;
            int peakHour = -1;
            long dayTotal = 0L;

            for (Text val : values) {
                String valStr = val.toString();
                int colonIdx = valStr.indexOf(':');
                if (colonIdx <= 0 || colonIdx >= valStr.length() - 1) {
                    continue;
                }

                try {
                    int h = Integer.parseInt(valStr.substring(0, colonIdx));
                    long tot = Long.parseLong(valStr.substring(colonIdx + 1));

                    dayTotal += tot;

                    // Tie-breaker rule (SRS §3.3.5): max total, tie -> smallest hour
                    if (tot > maxTotal) {
                        maxTotal = tot;
                        peakHour = h;
                    } else if (tot == maxTotal) {
                        if (h < peakHour) {
                            peakHour = h;
                        }
                    }
                } catch (NumberFormatException ignored) {
                }
            }

            if (peakHour >= 0 && collector != null) {
                outText.set(date + "\t" + peakHour + "\t" + maxTotal + "\t" + dayTotal);
                collector.write(OUTPUT_PEAKDAY, outText);
            }

        } else if (keyStr.startsWith(PeakHourMapper.HOUR_PREFIX)) {
            // Hour-of-Day Profile aggregation: key "H|HH", value "date:total"
            int hour;
            try {
                hour = Integer.parseInt(keyStr.substring(PeakHourMapper.HOUR_PREFIX.length()));
            } catch (NumberFormatException e) {
                return;
            }

            long totalEvents = 0L;
            int daysObserved = 0;

            for (Text val : values) {
                String valStr = val.toString();
                int colonIdx = valStr.indexOf(':');
                if (colonIdx <= 0 || colonIdx >= valStr.length() - 1) {
                    continue;
                }

                try {
                    long tot = Long.parseLong(valStr.substring(colonIdx + 1));
                    totalEvents += tot;
                    daysObserved++;
                } catch (NumberFormatException ignored) {
                }
            }

            if (daysObserved > 0 && collector != null) {
                outText.set(hour + "\t" + totalEvents + "\t" + daysObserved);
                collector.write(OUTPUT_HOUROFDAY, outText);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (collector != null) {
            collector.close();
        }
    }
}
