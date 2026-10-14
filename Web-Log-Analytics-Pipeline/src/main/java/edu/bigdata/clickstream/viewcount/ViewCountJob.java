package edu.bigdata.clickstream.viewcount;

import edu.bigdata.clickstream.writable.ViewCountWritable;
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
 * ViewCountJob configures and submits Job J3 per SRS §3.3.6 (FR-MR-12).
 *
 * Input:
 *   Raw clickstream CSV (any dt=* partition).
 *
 * Outputs (via MultipleOutputs subdirectories):
 *   1) products/part-r-*   : product_id \t category_code \t brand \t views
 *   2) categories/part-r-* : category_code \t views
 *
 * Execution constraints:
 *   - Uses associative/commutative ViewCountCombiner.
 *   - Default 4 reducers (overridable via clickstream.reducers.viewcount).
 *   - YARN queue "batch" by default.
 *   - Rejects existing output directory with exit code 1.
 *   - Suppresses default empty part-r-00000 files via LazyOutputFormat.
 */
public class ViewCountJob extends Configured implements Tool {

    public static final String DEFAULT_QUEUE = "batch";
    public static final String CONF_QUEUE_NAME = "mapreduce.job.queuename";
    public static final String CONF_REDUCERS_VIEWCOUNT = "clickstream.reducers.viewcount";
    public static final int DEFAULT_REDUCERS = 4;

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
            System.err.println("Usage: ViewCountJob <raw_clickstream_input> <output_path>");
            System.err.println("Error: Exactly two arguments required.");
            return 1;
        }

        Configuration conf = getConf();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Guard against output overwrite
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            System.err.println("Error: Output directory already exists: " + outputPath);
            System.err.println("Hadoop MapReduce requires a non-existent output directory to prevent data loss.");
            return 1;
        }

        // Configure YARN queue
        applyQueueConfiguration(conf);

        Job job = Job.getInstance(conf, "Job J3 — ViewCount");
        this.lastJob = job;
        job.setJarByClass(ViewCountJob.class);

        // Mapper configuration
        job.setMapperClass(ViewCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ViewCountWritable.class);

        // Combiner configuration (SRS §3.3.2 FR-MR-04)
        job.setCombinerClass(ViewCountCombiner.class);

        // Reducer configuration
        job.setReducerClass(ViewCountReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducers = conf.getInt(CONF_REDUCERS_VIEWCOUNT, conf.getInt("mapreduce.job.reduces", DEFAULT_REDUCERS));
        job.setNumReduceTasks(numReducers);

        // Input & Output Formats
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Prevent generation of empty part-r-00000 files
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // Register MultipleOutputs named outputs: "products" and "categories"
        MultipleOutputs.addNamedOutput(job, ViewCountReducer.OUTPUT_PRODUCTS, TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, ViewCountReducer.OUTPUT_CATEGORIES, TextOutputFormat.class, NullWritable.class, Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ViewCountJob(), args);
        System.exit(exitCode);
    }
}
