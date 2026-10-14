package edu.bigdata.clickstream.sessionfunnel;

import edu.bigdata.clickstream.writable.SessionAggWritable;
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
 * SessionFunnelJob configures and executes Job J5 per SRS §3.3.8 (FR-MR-14).
 *
 * Input:
 *   Raw clickstream CSV partition (e.g. /clickstream/raw/dt=YYYY-MM-DD/events.csv).
 *
 * Output:
 *   session_partial/part-r-*: date \t sessions \t cart \t purchase \t abandoned \t removes
 *
 * Execution constraints:
 *   - Uses associative/commutative SessionFunnelCombiner.
 *   - Default 8 reducers (configurable via clickstream.reducers.session per SRS §3.3.8).
 *   - YARN queue "batch" by default (FR-MR-03).
 *   - Rejects existing output directory with exit code 1 to prevent data loss.
 */
public class SessionFunnelJob extends Configured implements Tool {

    public static final String DEFAULT_QUEUE = "batch";
    public static final String CONF_QUEUE_NAME = "mapreduce.job.queuename";
    public static final String CONF_REDUCERS_SESSION = "clickstream.reducers.session";
    public static final int DEFAULT_REDUCERS_SESSION = 8;

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
            System.err.println("Usage: SessionFunnelJob <raw_clickstream_input> <output_path>");
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

        Job job = Job.getInstance(conf, "Job J5 — SessionFunnel");
        this.lastJob = job;
        job.setJarByClass(SessionFunnelJob.class);

        // Mapper configuration
        job.setMapperClass(SessionFunnelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SessionAggWritable.class);

        // Combiner configuration (SRS §3.3.2 FR-MR-04)
        job.setCombinerClass(SessionFunnelCombiner.class);

        // Reducer configuration
        job.setReducerClass(SessionFunnelReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Reducer count: default 8 per SRS §3.3.8, overridable via clickstream.reducers.session
        int numReducers = conf.getInt(CONF_REDUCERS_SESSION, conf.getInt("mapreduce.job.reduces", DEFAULT_REDUCERS_SESSION));
        job.setNumReduceTasks(numReducers);

        // Input and Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SessionFunnelJob(), args);
        System.exit(exitCode);
    }
}
