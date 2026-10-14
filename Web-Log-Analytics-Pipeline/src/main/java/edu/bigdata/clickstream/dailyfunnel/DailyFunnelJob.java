package edu.bigdata.clickstream.dailyfunnel;

import edu.bigdata.clickstream.writable.DailyFunnelWritable;
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
 * DailyFunnelJob executes Job J6 per SRS §3.3.9 (FR-MR-15).
 *
 * Input:
 *   J5 session_partial outputs (date \t sessions \t cart \t purchase \t abandoned \t removes).
 *
 * Output:
 *   funnel_daily (date \t sessions \t cart_sessions \t purchase_sessions \t abandoned_sessions \t remove_events).
 *
 * Constraints:
 *   - Uses 1 reducer to produce globally consolidated and sorted daily records.
 *   - Associative/commutative DailyFunnelCombiner.
 *   - YARN queue "batch" by default.
 *   - Overwrite protection: rejects existing output directory with exit code 1.
 */
public class DailyFunnelJob extends Configured implements Tool {

    public static final String DEFAULT_QUEUE = "batch";
    public static final String CONF_QUEUE_NAME = "mapreduce.job.queuename";
    public static final int DEFAULT_REDUCERS = 1;

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
            System.err.println("Usage: DailyFunnelJob <session_partial_input> <output_path>");
            System.err.println("Error: Exactly two arguments required.");
            return 1;
        }

        Configuration conf = getConf();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            System.err.println("Error: Output directory already exists: " + outputPath);
            return 1;
        }

        applyQueueConfiguration(conf);

        Job job = Job.getInstance(conf, "Job J6 — DailyFunnel");
        this.lastJob = job;
        job.setJarByClass(DailyFunnelJob.class);

        // Mapper configuration
        job.setMapperClass(DailyFunnelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DailyFunnelWritable.class);

        // Combiner configuration
        job.setCombinerClass(DailyFunnelCombiner.class);

        // Reducer configuration
        job.setReducerClass(DailyFunnelReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Exactly 1 reducer per SRS §3.3.9
        job.setNumReduceTasks(DEFAULT_REDUCERS);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new DailyFunnelJob(), args);
        System.exit(exitCode);
    }
}
