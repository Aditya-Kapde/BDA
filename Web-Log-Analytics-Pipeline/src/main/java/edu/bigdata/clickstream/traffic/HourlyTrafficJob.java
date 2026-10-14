package edu.bigdata.clickstream.traffic;

import edu.bigdata.clickstream.writable.EventCountWritable;
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
 * HourlyTrafficJob configures and executes Job J1 (HourlyTraffic) per SRS §3.3.4 (FR-MR-10).
 *
 * Usage:
 *   hadoop jar clickstream-mr-1.0.jar edu.bigdata.clickstream.traffic.HourlyTrafficJob \
 *     --input <input_path> --output <output_path>
 */
public class HourlyTrafficJob extends Configured implements Tool {

    public static final String CONF_REDUCERS_TRAFFIC = "clickstream.reducers.traffic";
    public static final int DEFAULT_REDUCERS_TRAFFIC = 4;
    public static final String DEFAULT_QUEUE_NAME = "batch";

    protected Job lastJob;

    public Job getLastJob() {
        return lastJob;
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = null;
        String outputPath = null;

        // Parse CLI arguments (--input/--output or positional)
        for (int i = 0; i < args.length; i++) {
            if ("--input".equals(args[i]) && i + 1 < args.length) {
                inputPath = args[++i];
            } else if ("--output".equals(args[i]) && i + 1 < args.length) {
                outputPath = args[++i];
            } else if (inputPath == null && !args[i].startsWith("-")) {
                inputPath = args[i];
            } else if (outputPath == null && !args[i].startsWith("-")) {
                outputPath = args[i];
            }
        }

        if (inputPath == null || outputPath == null) {
            System.err.println("Error: Missing required input or output paths.");
            System.err.println("Usage: HourlyTrafficJob --input <input_path> --output <output_path>");
            return 1;
        }

        Configuration conf = getConf();
        if (conf == null) {
            conf = new Configuration();
        }

        Path in = new Path(inputPath);
        Path out = new Path(outputPath);
        FileSystem fs = out.getFileSystem(conf);

        // Protection: Never silently overwrite an existing output directory
        if (fs.exists(out)) {
            System.err.println("Error: Output directory already exists: " + out);
            System.err.println("Hadoop MapReduce requires a non-existent output directory to prevent data loss.");
            return 1;
        }

        Job job = Job.getInstance(conf, "J1_HourlyTraffic");
        this.lastJob = job;
        job.setJarByClass(HourlyTrafficJob.class);

        // Queue name setting (default: "batch" per SRS FR-MR-03)
        String queue = conf.get("mapreduce.job.queuename", DEFAULT_QUEUE_NAME);
        job.getConfiguration().set("mapreduce.job.queuename", queue);

        // Mapper, Combiner, Reducer bindings
        job.setMapperClass(HourlyTrafficMapper.class);
        job.setCombinerClass(HourlyTrafficCombiner.class);
        job.setReducerClass(HourlyTrafficReducer.class);

        // Map output contract
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(EventCountWritable.class);

        // Final output contract
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Reducer count (default 4 per SRS §3.3.4, configurable via clickstream.reducers.traffic)
        int reducers = conf.getInt(CONF_REDUCERS_TRAFFIC, DEFAULT_REDUCERS_TRAFFIC);
        job.setNumReduceTasks(reducers);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new HourlyTrafficJob(), args);
        System.exit(exitCode);
    }
}
