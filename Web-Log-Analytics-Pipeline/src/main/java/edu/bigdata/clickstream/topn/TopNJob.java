package edu.bigdata.clickstream.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * TopNJob configures and submits Job J4 per SRS §3.3.7 (FR-MR-13).
 *
 * Input:
 *   Job J3 output directory for either:
 *   - products:   <run_work>/viewcounts/products/
 *   - categories: <run_work>/viewcounts/categories/
 *
 * Output:
 *   - top_products:   rank \t product_id \t category_code \t brand \t views
 *   - top_categories: rank \t category_code \t views
 *
 * Execution constraints:
 *   - Parameterized by clickstream.topn (default 10).
 *   - Bounded in-mapper TreeMap candidate selection.
 *   - Strictly 1 reducer task (merges all mapper candidates globally).
 *   - YARN queue "batch" by default (FR-MR-03).
 *   - Rejects existing output directory with exit code 1 to protect data integrity.
 */
public class TopNJob extends Configured implements Tool {

    public static final String DEFAULT_QUEUE = "batch";
    public static final String CONF_QUEUE_NAME = "mapreduce.job.queuename";
    public static final String CONF_TOPN = "clickstream.topn";
    public static final int DEFAULT_TOPN = 10;

    protected Job lastJob;

    public Job getLastJob() {
        return lastJob;
    }

    public static void applyQueueConfiguration(Configuration conf) {
        String queue = conf.get(CONF_QUEUE_NAME);
        if (queue == null || queue.isEmpty() || "default".equalsIgnoreCase(queue)) {
            conf.set(CONF_QUEUE_NAME, DEFAULT_QUEUE);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TopNJob <j3_input_path> <output_path>");
            System.err.println("Error: Exactly two arguments required.");
            return 1;
        }

        Configuration conf = getConf();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Guard against accidental output overwrite
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            System.err.println("Error: Output directory already exists: " + outputPath);
            System.err.println("Hadoop MapReduce requires a non-existent output directory to prevent data loss.");
            return 1;
        }

        // Configure YARN queue
        applyQueueConfiguration(conf);

        Job job = Job.getInstance(conf, "Job J4 — TopN");
        this.lastJob = job;
        job.setJarByClass(TopNJob.class);

        // Mapper configuration
        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer configuration - strictly 1 reducer per SRS §3.3.7
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        // Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TopNJob(), args);
        System.exit(exitCode);
    }
}
