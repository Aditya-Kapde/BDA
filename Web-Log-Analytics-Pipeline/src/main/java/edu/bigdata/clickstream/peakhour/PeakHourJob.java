package edu.bigdata.clickstream.peakhour;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * PeakHourJob configures and executes Job J2 per SRS §3.3.5 (FR-MR-11).
 *
 * Input:
 *   Output directory of Job J1 (HourlyTraffic).
 *
 * Outputs (via MultipleOutputs):
 *   1) "peakday": date \t peak_hour \t peak_total \t day_total
 *   2) "hourofday": hour \t total_events \t days_observed
 *
 * Execution constraints:
 *   - Exactly 1 reducer task (merges all dates and hours).
 *   - YARN queue "batch" by default.
 *   - Rejects existing output directory with exit code 1 to prevent data loss.
 *   - Uses LazyOutputFormat to prevent empty part-r-00000 files.
 */
public class PeakHourJob extends Configured implements Tool {

    public static final String DEFAULT_QUEUE = "batch";
    public static final String CONF_QUEUE_NAME = "mapreduce.job.queuename";

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
            System.err.println("Usage: PeakHourJob <j1_input_path> <output_path>");
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

        Job job = Job.getInstance(conf, "Job J2 — PeakHour");
        this.lastJob = job;
        job.setJarByClass(PeakHourJob.class);

        // Mapper configuration
        job.setMapperClass(PeakHourMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer configuration - strictly 1 reducer per SRS §3.3.5
        job.setReducerClass(PeakHourReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        // Formats and I/O
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Prevent generation of empty part-r-00000 by using LazyOutputFormat
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // Configure MultipleOutputs for named outputs: "peakday" and "hourofday"
        MultipleOutputs.addNamedOutput(job, PeakHourReducer.OUTPUT_PEAKDAY, TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, PeakHourReducer.OUTPUT_HOUROFDAY, TextOutputFormat.class, NullWritable.class, Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PeakHourJob(), args);
        System.exit(exitCode);
    }
}
