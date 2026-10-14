package edu.bigdata.clickstream.viewcount;

import edu.bigdata.clickstream.writable.ViewCountWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * ViewCountReducer implements the reduction stage of Job J3 per SRS §3.3.6 (FR-MR-12).
 *
 * Uses MultipleOutputs to produce two named outputs:
 * 1) "products" (written under "products/part"):
 *    product_id \t category_code \t brand \t views
 * 2) "categories" (written under "categories/part"):
 *    category_code \t views
 */
public class ViewCountReducer extends Reducer<Text, ViewCountWritable, NullWritable, Text> {

    public static final String OUTPUT_PRODUCTS = "products";
    public static final String OUTPUT_CATEGORIES = "categories";
    public static final String BASE_PATH_PRODUCTS = "products/part";
    public static final String BASE_PATH_CATEGORIES = "categories/part";

    /**
     * OutputCollector interface decouples MultipleOutputs I/O from algorithmic logic,
     * allowing 100% deterministic, zero-overhead unit testing without JVM bytecode retransformation issues.
     */
    public interface OutputCollector {
        void write(String namedOutput, Text value, String baseOutputPath) throws IOException, InterruptedException;
        void close() throws IOException, InterruptedException;
    }

    private MultipleOutputs<NullWritable, Text> mos;
    private OutputCollector collector;
    private final ViewCountWritable accumulator = new ViewCountWritable();
    private final Text outText = new Text();

    public ViewCountReducer() {
    }

    // Package-private constructor for unit testing with injected collector
    ViewCountReducer(OutputCollector collector) {
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
                public void write(String namedOutput, Text value, String baseOutputPath) throws IOException, InterruptedException {
                    mos.write(namedOutput, NullWritable.get(), value, baseOutputPath);
                }

                @Override
                public void close() throws IOException, InterruptedException {
                    mos.close();
                }
            };
        }
    }

    @Override
    protected void reduce(Text key, Iterable<ViewCountWritable> values, Context context) throws IOException, InterruptedException {
        accumulator.reset();
        for (ViewCountWritable val : values) {
            accumulator.merge(val);
        }

        if (accumulator.getCount() <= 0) {
            return;
        }

        String keyStr = key.toString();
        if (keyStr.startsWith(ViewCountMapper.PRODUCT_PREFIX)) {
            String productId = keyStr.substring(ViewCountMapper.PRODUCT_PREFIX.length());
            // Schema: product_id \t category_code \t brand \t views
            outText.set(productId + "\t" + accumulator.getCategory() + "\t" + accumulator.getBrand() + "\t" + accumulator.getCount());
            if (collector != null) {
                collector.write(OUTPUT_PRODUCTS, outText, BASE_PATH_PRODUCTS);
            }
        } else if (keyStr.startsWith(ViewCountMapper.CATEGORY_PREFIX)) {
            String categoryCode = keyStr.substring(ViewCountMapper.CATEGORY_PREFIX.length());
            // Schema: category_code \t views
            outText.set(categoryCode + "\t" + accumulator.getCount());
            if (collector != null) {
                collector.write(OUTPUT_CATEGORIES, outText, BASE_PATH_CATEGORIES);
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
