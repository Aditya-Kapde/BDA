package edu.bigdata.clickstream;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.dailyfunnel.DailyFunnelJob;
import edu.bigdata.clickstream.peakhour.PeakHourJob;
import edu.bigdata.clickstream.sessionfunnel.SessionFunnelJob;
import edu.bigdata.clickstream.topn.TopNJob;
import edu.bigdata.clickstream.traffic.HourlyTrafficJob;
import edu.bigdata.clickstream.viewcount.ViewCountJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PipelineDriver orchestrates the execution of Hadoop MapReduce Clickstream jobs (J1..J6)
 * in strict dependency order per PRD §4, SRS §3.3.10, and SRS §3.5.6.
 *
 * Dependency Order:
 *   1. J1 HourlyTraffic   (raw input -> work_dir/traffic)
 *   2. J2 PeakHour        (work_dir/traffic -> work_dir/peak)
 *   3. J3 ViewCount       (raw input -> work_dir/viewcounts)
 *   4. J4 TopN Products   (work_dir/viewcounts/products -> work_dir/top_products)
 *   5. J4 TopN Categories (work_dir/viewcounts/categories -> work_dir/top_categories)
 *   6. J5 SessionFunnel   (raw input -> work_dir/session_partial)
 *   7. J6 DailyFunnel     (work_dir/session_partial -> work_dir/funnel_daily)
 *
 * Invariants:
 *   - FR-MR-07: Σ(J1.total + J1.malformed) == TOTAL_RECORDS - HEADER_SKIPPED
 *   - Overwrite protection: Refuses to overwrite existing work-dir unless --force is given.
 *   - Idempotent and atomic metadata creation: writes run_meta.json.tmp and renames to run_meta.json.
 */
public class PipelineDriver extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineDriver.class);

    public static final String SCHEMA_VERSION = "1.0";
    public static final String APP_VERSION = "1.0";

    public static final String DEFAULT_QUEUE = "batch";
    public static final int DEFAULT_TOPN = 10;
    public static final int DEFAULT_REDUCERS_TRAFFIC = 4;
    public static final int DEFAULT_REDUCERS_SESSION = 8;

    public static final String CONF_QUEUE = "mapreduce.job.queuename";
    public static final String CONF_TOPN = "clickstream.topn";
    public static final String CONF_REDUCERS_TRAFFIC = "clickstream.reducers.traffic";
    public static final String CONF_REDUCERS_SESSION = "clickstream.reducers.session";

    public static final String DIR_TRAFFIC = "traffic";
    public static final String DIR_PEAK = "peak";
    public static final String DIR_VIEWCOUNTS = "viewcounts";
    public static final String DIR_TOP_PRODUCTS = "top_products";
    public static final String DIR_TOP_CATEGORIES = "top_categories";
    public static final String DIR_SESSION_PARTIAL = "session_partial";
    public static final String DIR_FUNNEL_DAILY = "funnel_daily";

    public static final String META_FILE = "run_meta.json";
    public static final String META_TMP = "run_meta.json.tmp";
    public static final String LOCK_FILE_NAME = ".pipeline.lock";
    public static final int EXIT_LOCK_COLLISION = 75;

    // Data structure for recording individual job execution metadata
    public static class JobExecutionRecord {
        public String name;
        public String jobId;
        public String status = "UNKNOWN";
        public Instant start;
        public Instant end;
        public long durationSec;
        public String input;
        public String output;
        public Map<String, Long> counters = new LinkedHashMap<>();
        public String error;
    }

    public static class PipelineContext {
        public String runId;
        public String inputPath;
        public String workDir;
        public String reportDir;
        public String queue = DEFAULT_QUEUE;
        public int topN = DEFAULT_TOPN;
        public int reducersTraffic = DEFAULT_REDUCERS_TRAFFIC;
        public int reducersSession = DEFAULT_REDUCERS_SESSION;
        public boolean force = false;
        public boolean help = false;

        public Instant start;
        public Instant end;
        public long durationSec;
        public String status = "UNKNOWN";
        public boolean invariantOk = false;
        public String failedJob;
        public String error;
        public List<JobExecutionRecord> jobs = new ArrayList<>();
    }

    @Override
    public int run(String[] args) throws Exception {
        PipelineContext ctx = new PipelineContext();
        int parseResult = parseArgs(args, ctx);
        if (parseResult != 0) {
            return parseResult;
        }

        if (ctx.help) {
            printUsage();
            return 0;
        }

        Configuration conf = getConf();
        if (conf == null) {
            conf = new Configuration();
            setConf(conf);
        }

        // Apply effective configuration parameters
        conf.set(CONF_QUEUE, ctx.queue);
        conf.setInt(CONF_TOPN, ctx.topN);
        conf.setInt(CONF_REDUCERS_TRAFFIC, ctx.reducersTraffic);
        conf.setInt(CONF_REDUCERS_SESSION, ctx.reducersSession);

        Path inPath = new Path(ctx.inputPath);
        Path workPath = new Path(ctx.workDir);

        FileSystem inFs = inPath.getFileSystem(conf);
        FileSystem workFs = workPath.getFileSystem(conf);

        // Pre-flight check 1: Input path must exist
        if (!inFs.exists(inPath)) {
            System.err.println("Error: Input path does not exist: " + ctx.inputPath);
            return 1;
        }

        // Pre-flight check 2: Prevent raw input deletion / overwrite
        if (inPath.equals(workPath)) {
            System.err.println("Error: Work directory cannot be identical to raw input path: " + ctx.workDir);
            return 1;
        }

        // Compute stable lock path outside workPath to prevent force-cleanup deletion
        Path lockPath = computeLockPath(workPath);
        PipelineLock pipelineLock = new PipelineLock(workFs, lockPath);
        if (!pipelineLock.tryAcquire(ctx.runId)) {
            System.err.println("Error: Another pipeline run is currently active for work directory (lock collision): " + ctx.workDir);
            return EXIT_LOCK_COLLISION;
        }

        int exitCode = 0;
        try {
            // Pre-flight check 3: Work directory existence and overwrite protection
            if (workFs.exists(workPath)) {
                if (!ctx.force) {
                    System.err.println("Error: Pipeline work directory already exists: " + ctx.workDir);
                    System.err.println("Refusing to overwrite existing data without explicit --force or --overwrite option.");
                    exitCode = 1;
                    return exitCode;
                } else {
                    // Safety guard against accidental deletion of root or critical paths
                    String normWork = workPath.toUri().getPath();
                    if (normWork == null || normWork.equals("/") || normWork.trim().isEmpty()) {
                        System.err.println("Error: Refusing to delete root path as work directory: " + ctx.workDir);
                        exitCode = 1;
                        return exitCode;
                    }
                    LOG.info("Overwriting existing work directory: {}", ctx.workDir);
                    workFs.delete(workPath, true);
                }
            }

            // Create work root
            try {
                workFs.mkdirs(workPath);
            } catch (Throwable t) {
                File localWork = getLocalFile(workPath);
                if (localWork != null && !localWork.exists()) {
                    localWork.mkdirs();
                }
            }

            ctx.start = Instant.now();
            LOG.info("Starting Clickstream MapReduce Pipeline run '{}' on queue '{}'", ctx.runId, ctx.queue);

            do {
                // =========================================================================
                // 1. Job J1: HourlyTraffic
                // =========================================================================
                Path trafficPath = new Path(workPath, DIR_TRAFFIC);
                JobExecutionRecord j1Rec = executeJob(
                    "J1_HourlyTraffic",
                    new HourlyTrafficJob(),
                    new String[]{"--input", inPath.toString(), "--output", trafficPath.toString()},
                    conf,
                    inPath.toString(),
                    trafficPath.toString()
                );
                ctx.jobs.add(j1Rec);
                if (!"SUCCESS".equals(j1Rec.status)) {
                    ctx.failedJob = j1Rec.name;
                    ctx.error = "Job J1 (HourlyTraffic) failed: " + j1Rec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // Verify invariant FR-MR-07: Σ(J1.total + J1.malformed) == TOTAL_RECORDS - HEADER_SKIPPED
                boolean invOk = verifyReconciliationInvariant(workFs, trafficPath, j1Rec.counters, ctx);
                if (!invOk) {
                    ctx.invariantOk = false;
                    ctx.failedJob = "J1_HourlyTraffic";
                    ctx.status = "FAILED";
                    ctx.error = "Reconciliation invariant violation: Σ(J1.total + J1.malformed) != TOTAL_RECORDS - HEADER_SKIPPED";
                    LOG.error(ctx.error);
                    exitCode = 2;
                    break;
                }
                ctx.invariantOk = true;

                // Validate upstream output for downstream J2
                if (!hasValidOutputFiles(workFs, trafficPath)) {
                    ctx.failedJob = "J1_HourlyTraffic";
                    ctx.error = "Upstream J1 traffic output missing or empty in " + trafficPath;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // =========================================================================
                // 2. Job J2: PeakHour
                // =========================================================================
                Path peakPath = new Path(workPath, DIR_PEAK);
                JobExecutionRecord j2Rec = executeJob(
                    "J2_PeakHour",
                    new PeakHourJob(),
                    new String[]{trafficPath.toString(), peakPath.toString()},
                    conf,
                    trafficPath.toString(),
                    peakPath.toString()
                );
                ctx.jobs.add(j2Rec);
                if (!"SUCCESS".equals(j2Rec.status)) {
                    ctx.failedJob = j2Rec.name;
                    ctx.error = "Job J2 (PeakHour) failed: " + j2Rec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // =========================================================================
                // 3. Job J3: ViewCount
                // =========================================================================
                Path viewcountsPath = new Path(workPath, DIR_VIEWCOUNTS);
                JobExecutionRecord j3Rec = executeJob(
                    "J3_ViewCount",
                    new ViewCountJob(),
                    new String[]{inPath.toString(), viewcountsPath.toString()},
                    conf,
                    inPath.toString(),
                    viewcountsPath.toString()
                );
                ctx.jobs.add(j3Rec);
                if (!"SUCCESS".equals(j3Rec.status)) {
                    ctx.failedJob = j3Rec.name;
                    ctx.error = "Job J3 (ViewCount) failed: " + j3Rec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // =========================================================================
                // 4. Job J4: TopN Products
                // =========================================================================
                Path j3ProductsIn = new Path(viewcountsPath, "products");
                if (!hasValidOutputFiles(workFs, j3ProductsIn)) {
                    ctx.failedJob = "J3_ViewCount";
                    ctx.error = "Upstream J3 products output missing or empty in " + j3ProductsIn;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                Path topProductsOut = new Path(workPath, DIR_TOP_PRODUCTS);
                JobExecutionRecord j4ProdRec = executeJob(
                    "J4_TopN_Products",
                    new TopNJob(),
                    new String[]{j3ProductsIn.toString(), topProductsOut.toString()},
                    conf,
                    j3ProductsIn.toString(),
                    topProductsOut.toString()
                );
                ctx.jobs.add(j4ProdRec);
                if (!"SUCCESS".equals(j4ProdRec.status)) {
                    ctx.failedJob = j4ProdRec.name;
                    ctx.error = "Job J4 (TopN Products) failed: " + j4ProdRec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // =========================================================================
                // 5. Job J4: TopN Categories
                // =========================================================================
                Path j3CategoriesIn = new Path(viewcountsPath, "categories");
                if (!hasValidOutputFiles(workFs, j3CategoriesIn)) {
                    ctx.failedJob = "J3_ViewCount";
                    ctx.error = "Upstream J3 categories output missing or empty in " + j3CategoriesIn;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                Path topCategoriesOut = new Path(workPath, DIR_TOP_CATEGORIES);
                JobExecutionRecord j4CatRec = executeJob(
                    "J4_TopN_Categories",
                    new TopNJob(),
                    new String[]{j3CategoriesIn.toString(), topCategoriesOut.toString()},
                    conf,
                    j3CategoriesIn.toString(),
                    topCategoriesOut.toString()
                );
                ctx.jobs.add(j4CatRec);
                if (!"SUCCESS".equals(j4CatRec.status)) {
                    ctx.failedJob = j4CatRec.name;
                    ctx.error = "Job J4 (TopN Categories) failed: " + j4CatRec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // =========================================================================
                // 6. Job J5: SessionFunnel
                // =========================================================================
                Path sessionPartialPath = new Path(workPath, DIR_SESSION_PARTIAL);
                JobExecutionRecord j5Rec = executeJob(
                    "J5_SessionFunnel",
                    new SessionFunnelJob(),
                    new String[]{inPath.toString(), sessionPartialPath.toString()},
                    conf,
                    inPath.toString(),
                    sessionPartialPath.toString()
                );
                ctx.jobs.add(j5Rec);
                if (!"SUCCESS".equals(j5Rec.status)) {
                    ctx.failedJob = j5Rec.name;
                    ctx.error = "Job J5 (SessionFunnel) failed: " + j5Rec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                // =========================================================================
                // 7. Job J6: DailyFunnel
                // =========================================================================
                if (!hasValidOutputFiles(workFs, sessionPartialPath)) {
                    ctx.failedJob = "J5_SessionFunnel";
                    ctx.error = "Upstream J5 session_partial output missing or empty in " + sessionPartialPath;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                Path funnelDailyPath = new Path(workPath, DIR_FUNNEL_DAILY);
                JobExecutionRecord j6Rec = executeJob(
                    "J6_DailyFunnel",
                    new DailyFunnelJob(),
                    new String[]{sessionPartialPath.toString(), funnelDailyPath.toString()},
                    conf,
                    sessionPartialPath.toString(),
                    funnelDailyPath.toString()
                );
                ctx.jobs.add(j6Rec);
                if (!"SUCCESS".equals(j6Rec.status)) {
                    ctx.failedJob = j6Rec.name;
                    ctx.error = "Job J6 (DailyFunnel) failed: " + j6Rec.error;
                    ctx.status = "FAILED";
                    exitCode = 1;
                    break;
                }

                ctx.status = "SUCCESS";
                LOG.info("All pipeline jobs J1..J6 completed successfully for run '{}'", ctx.runId);
                exitCode = 0;

            } while (false);

        } catch (Throwable t) {
            ctx.status = "FAILED";
            ctx.error = "Unhandled pipeline exception: " + t.getMessage();
            LOG.error("Pipeline execution failed with exception", t);
            exitCode = 1;
        } finally {
            if (ctx.start != null) {
                ctx.end = Instant.now();
                ctx.durationSec = Math.max(0, ctx.end.getEpochSecond() - ctx.start.getEpochSecond());
                try {
                    writeRunMetadata(workFs, workPath, ctx);
                } catch (Throwable me) {
                    LOG.error("Failed to write {}: {}", META_FILE, me.getMessage(), me);
                    if (exitCode == 0) {
                        exitCode = 1;
                    }
                }
            }
            pipelineLock.close();
        }
        return exitCode;
    }

    @FunctionalInterface
    public interface JobRunner {
        int runJob(Tool jobTool, String[] args, Configuration conf) throws Exception;
    }

    private JobRunner jobRunner = (jobTool, args, conf) -> ToolRunner.run(new Configuration(conf), jobTool, args);

    public void setJobRunner(JobRunner runner) {
        this.jobRunner = runner;
    }

    public JobRunner getJobRunner() {
        return this.jobRunner;
    }

    /**
     * Executes a single Tool job, capturing start/end timings, duration, status, and counters.
     */
    protected JobExecutionRecord executeJob(
            String name,
            Tool jobTool,
            String[] args,
            Configuration conf,
            String inputPath,
            String outputPath) {

        JobExecutionRecord rec = new JobExecutionRecord();
        rec.name = name;
        rec.input = inputPath;
        rec.output = outputPath;
        rec.start = Instant.now();

        try {
            LOG.info(">>> Launching [{}] with input='{}', output='{}'", name, inputPath, outputPath);
            int code = jobRunner.runJob(jobTool, args, conf);
            rec.end = Instant.now();
            rec.durationSec = Math.max(0, rec.end.getEpochSecond() - rec.start.getEpochSecond());

            // Extract Job details if driver implements getLastJob()
            Job job = null;
            if (jobTool instanceof HourlyTrafficJob) {
                job = ((HourlyTrafficJob) jobTool).getLastJob();
            } else if (jobTool instanceof PeakHourJob) {
                job = ((PeakHourJob) jobTool).getLastJob();
            } else if (jobTool instanceof ViewCountJob) {
                job = ((ViewCountJob) jobTool).getLastJob();
            } else if (jobTool instanceof TopNJob) {
                job = ((TopNJob) jobTool).getLastJob();
            } else if (jobTool instanceof SessionFunnelJob) {
                job = ((SessionFunnelJob) jobTool).getLastJob();
            } else if (jobTool instanceof DailyFunnelJob) {
                job = ((DailyFunnelJob) jobTool).getLastJob();
            }

            if (job != null) {
                if (job.getJobID() != null) {
                    rec.jobId = job.getJobID().toString();
                }
                captureCounters(job.getCounters(), rec.counters);
            }

            if (code == 0) {
                rec.status = "SUCCESS";
                LOG.info("<<< [{}] SUCCEEDED in {}s (job_id: {})", name, rec.durationSec, rec.jobId != null ? rec.jobId : "local");
            } else {
                rec.status = "FAILED";
                rec.error = "Job exited with code " + code;
                LOG.error("<<< [{}] FAILED with exit code {}", name, code);
            }
        } catch (Throwable t) {
            rec.end = Instant.now();
            rec.durationSec = Math.max(0, rec.end.getEpochSecond() - rec.start.getEpochSecond());
            rec.status = "FAILED";
            rec.error = t.getMessage();
            LOG.error("<<< [{}] threw exception", name, t);
        }

        return rec;
    }

    /**
     * Extracts counters from Hadoop Counters into a map.
     */
    public static void captureCounters(Counters counters, Map<String, Long> target) {
        if (counters == null) {
            return;
        }
        try {
            // First capture QualityCounter group with clean names
            CounterGroup qGroup = counters.getGroup(QualityCounter.GROUP_NAME);
            if (qGroup != null) {
                for (Counter c : qGroup) {
                    target.put(c.getName(), c.getValue());
                }
            }
            // Also capture standard framework counters for observability
            for (CounterGroup g : counters) {
                if (!QualityCounter.GROUP_NAME.equals(g.getName())) {
                    for (Counter c : g) {
                        target.put(c.getName(), c.getValue());
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract full counters: {}", e.getMessage());
        }
    }

    /**
     * Checks if directory exists and contains valid non-hidden output files.
     */
    public static boolean hasValidOutputFiles(FileSystem fs, Path dir) throws IOException {
        if (dir == null) {
            return false;
        }
        try {
            if (!fs.exists(dir)) {
                return false;
            }
        } catch (Throwable t) {
            File localDir = new File(dir.toUri().getPath());
            if (!localDir.exists()) {
                return false;
            }
        }

        FileStatus[] statuses = null;
        try {
            statuses = fs.listStatus(dir, p -> {
                String name = p.getName();
                return !name.startsWith("_") && !name.startsWith(".");
            });
        } catch (Throwable t) {
            File localDir = new File(dir.toUri().getPath());
            File[] files = localDir.listFiles((d, name) -> !name.startsWith("_") && !name.startsWith("."));
            if (files != null) {
                statuses = new FileStatus[files.length];
                for (int i = 0; i < files.length; i++) {
                    statuses[i] = new FileStatus(files[i].length(), files[i].isDirectory(), 1, 1024,
                            files[i].lastModified(), new Path(files[i].getAbsolutePath()));
                }
            }
        }

        return statuses != null && statuses.length > 0;
    }

    public static File getLocalFile(Path path) {
        if (path == null) {
            return null;
        }
        try {
            java.net.URI uri = path.toUri();
            if (uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme())) {
                String p = uri.getPath();
                if (p != null) {
                    if (p.length() >= 3 && p.charAt(0) == '/' && Character.isLetter(p.charAt(1)) && p.charAt(2) == ':') {
                        p = p.substring(1);
                    }
                    return new File(p);
                }
            }
        } catch (Throwable ignored) {
        }
        return new File(path.toString());
    }

    public static Path computeLockPath(Path workPath) {
        if (workPath == null) {
            return new Path(".pipeline.lock");
        }
        Path parent = workPath.getParent();
        String name = workPath.getName();
        if (parent != null) {
            return new Path(parent, "." + name + LOCK_FILE_NAME);
        } else {
            return new Path("." + name + LOCK_FILE_NAME);
        }
    }

    public static class PipelineLock implements AutoCloseable {
        private final FileSystem fs;
        private final Path lockPath;
        private final File localLockFile;
        private java.io.RandomAccessFile raf;
        private java.nio.channels.FileLock fileLock;
        private boolean acquired = false;

        public PipelineLock(FileSystem fs, Path lockPath) {
            this.fs = fs;
            this.lockPath = lockPath;
            this.localLockFile = getLocalFile(lockPath);
        }

        public boolean tryAcquire(String runId) {
            if (localLockFile != null) {
                try {
                    File parent = localLockFile.getParentFile();
                    if (parent != null && !parent.exists()) {
                        parent.mkdirs();
                    }
                    this.raf = new java.io.RandomAccessFile(localLockFile, "rw");
                    this.fileLock = raf.getChannel().tryLock();
                    if (this.fileLock == null) {
                        closeQuietly();
                        return false;
                    }
                    raf.setLength(0);
                    raf.write(("run_id=" + runId + "\ntimestamp=" + Instant.now() + "\n").getBytes(StandardCharsets.UTF_8));
                    this.acquired = true;
                    return true;
                } catch (java.nio.channels.OverlappingFileLockException e) {
                    closeQuietly();
                    return false;
                } catch (Throwable t) {
                    // Try Hadoop FileSystem createNewFile fallback
                }
            }

            try {
                if (fs != null) {
                    if (!fs.createNewFile(lockPath)) {
                        return false;
                    }
                    try (BufferedWriter writer = new BufferedWriter(
                            new OutputStreamWriter(fs.create(lockPath, true), StandardCharsets.UTF_8))) {
                        writer.write("run_id=" + runId + "\ntimestamp=" + Instant.now() + "\n");
                    }
                    this.acquired = true;
                    return true;
                }
            } catch (Throwable t) {
                return false;
            }
            return false;
        }

        public boolean isAcquired() {
            return acquired;
        }

        public Path getLockPath() {
            return lockPath;
        }

        @Override
        public void close() {
            if (!acquired) {
                return;
            }
            try {
                if (fileLock != null && fileLock.isValid()) {
                    fileLock.release();
                }
            } catch (Throwable ignored) {
            }
            closeQuietly();
            try {
                if (fs != null && fs.exists(lockPath)) {
                    fs.delete(lockPath, false);
                }
            } catch (Throwable ignored) {
            }
            try {
                if (localLockFile != null && localLockFile.exists()) {
                    localLockFile.delete();
                }
            } catch (Throwable ignored) {
            }
            this.acquired = false;
        }

        private void closeQuietly() {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (Throwable ignored) {
            }
            raf = null;
            fileLock = null;
        }
    }

    public static boolean isPipelineLocked(FileSystem fs, Path lockPath) {
        if (lockPath == null) {
            return false;
        }
        File local = getLocalFile(lockPath);
        if (local != null && local.exists()) {
            try (java.io.RandomAccessFile testRaf = new java.io.RandomAccessFile(local, "rw");
                 java.nio.channels.FileLock testLock = testRaf.getChannel().tryLock()) {
                if (testLock == null) {
                    return true;
                }
            } catch (java.nio.channels.OverlappingFileLockException e) {
                return true;
            } catch (Throwable ignored) {
                return true;
            }
        }
        try {
            if (fs != null && fs.exists(lockPath)) {
                return true;
            }
        } catch (Throwable ignored) {
        }
        return false;
    }

    public static void createPipelineLock(FileSystem fs, Path lockPath, String runId) throws IOException {
        PipelineLock lock = new PipelineLock(fs, lockPath);
        lock.tryAcquire(runId);
    }

    public static void deletePipelineLock(FileSystem fs, Path lockPath) {
        File local = getLocalFile(lockPath);
        if (local != null && local.exists()) {
            local.delete();
        }
        try {
            if (fs != null && fs.exists(lockPath)) {
                fs.delete(lockPath, false);
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * Verifies invariant FR-MR-07: Σ(J1.total + J1.malformed) == TOTAL_RECORDS - HEADER_SKIPPED.
     */
    public static boolean verifyReconciliationInvariant(
            FileSystem fs,
            Path trafficPath,
            Map<String, Long> counters,
            PipelineContext ctx) throws IOException {

        long sumTotalAndMalformed = 0;
        FileStatus[] statuses = null;
        try {
            statuses = fs.listStatus(trafficPath, path -> {
                String name = path.getName();
                return !name.startsWith("_") && !name.startsWith(".");
            });
        } catch (Throwable t) {
            File localTraffic = new File(trafficPath.toUri().getPath());
            File[] files = localTraffic.listFiles((dir, name) -> !name.startsWith("_") && !name.startsWith("."));
            if (files != null) {
                statuses = new FileStatus[files.length];
                for (int i = 0; i < files.length; i++) {
                    statuses[i] = new FileStatus(files[i].length(), files[i].isDirectory(), 1, 1024,
                            files[i].lastModified(), new Path(files[i].getAbsolutePath()));
                }
            }
        }

        if (statuses != null) {
            for (FileStatus status : statuses) {
                if (status.isFile()) {
                    InputStream inStream = null;
                    try {
                        inStream = fs.open(status.getPath());
                    } catch (Throwable t) {
                        File localFile = new File(status.getPath().toUri().getPath());
                        if (localFile.exists()) {
                            inStream = new FileInputStream(localFile);
                        }
                    }

                    if (inStream != null) {
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(inStream, StandardCharsets.UTF_8))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                line = line.trim();
                                if (line.isEmpty()) continue;
                                String[] cols = line.split("\t");
                                if (cols.length >= 8) {
                                    try {
                                        long total = Long.parseLong(cols[2].trim());
                                        long malformed = Long.parseLong(cols[7].trim());
                                        sumTotalAndMalformed += (total + malformed);
                                    } catch (NumberFormatException ignored) {
                                        // ignore header or malformed lines
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        long totalRecords = counters.getOrDefault(QualityCounter.TOTAL_RECORDS.name(), 0L);
        long headerSkipped = counters.getOrDefault(QualityCounter.HEADER_SKIPPED.name(), 0L);
        long expected = totalRecords - headerSkipped;

        LOG.info("Reconciliation Invariant Check: Σ(J1.total + J1.malformed) = {}, TOTAL_RECORDS ({}) - HEADER_SKIPPED ({}) = {}",
                sumTotalAndMalformed, totalRecords, headerSkipped, expected);

        boolean passed = (sumTotalAndMalformed == expected);
        if (!passed) {
            LOG.error("Invariant MISMATCH: J1 output record sum {} != expected {}", sumTotalAndMalformed, expected);
        }
        return passed;
    }

    /**
     * Writes run_meta.json atomically to work_dir using run_meta.json.tmp and atomic rename.
     */
    public static void writeRunMetadata(FileSystem fs, Path workPath, PipelineContext ctx) throws IOException {
        Path tmpFile = new Path(workPath, META_TMP);
        Path finalFile = new Path(workPath, META_FILE);

        String json = buildRunMetaJson(ctx);

        try {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(tmpFile, true), StandardCharsets.UTF_8))) {
                writer.write(json);
            }

            if (fs.exists(finalFile)) {
                fs.delete(finalFile, false);
            }
            boolean renamed = fs.rename(tmpFile, finalFile);
            if (!renamed) {
                throw new IOException("Failed to atomically rename " + tmpFile + " to " + finalFile);
            }
        } catch (Throwable t) {
            // Local fallback for Windows filesystem when winutils.exe is absent
            File parentDir = new File(workPath.toUri().getPath());
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            File tmpLocal = new File(parentDir, META_TMP);
            File finalLocal = new File(parentDir, META_FILE);
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new java.io.FileOutputStream(tmpLocal), StandardCharsets.UTF_8))) {
                writer.write(json);
            }
            if (finalLocal.exists()) {
                finalLocal.delete();
            }
            if (!tmpLocal.renameTo(finalLocal)) {
                java.nio.file.Files.move(tmpLocal.toPath(), finalLocal.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        }
        LOG.info("Successfully recorded run metadata in {}", finalFile);
    }

    /**
     * Builds valid, versioned JSON according to SRS §3.5.6.
     */
    public static String buildRunMetaJson(PipelineContext ctx) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        appendField(sb, "schema_version", SCHEMA_VERSION, 1, true);
        appendField(sb, "app_version", APP_VERSION, 1, true);
        appendField(sb, "run_id", ctx.runId, 1, true);
        appendField(sb, "status", ctx.status, 1, true);
        appendField(sb, "start", ctx.start != null ? ctx.start.toString() : "", 1, true);
        appendField(sb, "end", ctx.end != null ? ctx.end.toString() : "", 1, true);
        appendNumber(sb, "duration_sec", ctx.durationSec, 1, true);
        appendField(sb, "input", ctx.inputPath, 1, true);
        appendField(sb, "work_dir", ctx.workDir, 1, true);
        if (ctx.reportDir != null) {
            appendField(sb, "report_dir", ctx.reportDir, 1, true);
        }
        appendField(sb, "queue", ctx.queue, 1, true);
        appendBool(sb, "invariant_ok", ctx.invariantOk, 1, true);

        if ("FAILED".equals(ctx.status)) {
            if (ctx.failedJob != null) {
                appendField(sb, "failed_job", ctx.failedJob, 1, true);
            }
            if (ctx.error != null) {
                appendField(sb, "error", ctx.error, 1, true);
            }
        }

        // effective configuration
        sb.append("  \"effective_config\": {\n");
        appendNumber(sb, "topn", ctx.topN, 2, true);
        appendNumber(sb, "reducers_traffic", ctx.reducersTraffic, 2, true);
        appendNumber(sb, "reducers_session", ctx.reducersSession, 2, true);
        appendField(sb, "queue", ctx.queue, 2, false);
        sb.append("  },\n");

        // jobs array
        sb.append("  \"jobs\": [\n");
        for (int i = 0; i < ctx.jobs.size(); i++) {
            JobExecutionRecord j = ctx.jobs.get(i);
            sb.append("    {\n");
            appendField(sb, "name", j.name, 3, true);
            appendField(sb, "job_id", j.jobId != null ? j.jobId : "local", 3, true);
            appendField(sb, "status", j.status, 3, true);
            appendField(sb, "start", j.start != null ? j.start.toString() : "", 3, true);
            appendField(sb, "end", j.end != null ? j.end.toString() : "", 3, true);
            appendNumber(sb, "duration_sec", j.durationSec, 3, true);
            appendField(sb, "input", j.input, 3, true);
            appendField(sb, "output", j.output, 3, true);
            if (j.error != null) {
                appendField(sb, "error", j.error, 3, true);
            }

            // counters object
            sb.append("      \"counters\": {");
            if (j.counters.isEmpty()) {
                sb.append("}\n");
            } else {
                sb.append("\n");
                int cIdx = 0;
                int cTotal = j.counters.size();
                for (Map.Entry<String, Long> entry : j.counters.entrySet()) {
                    cIdx++;
                    sb.append("        \"").append(escapeJson(entry.getKey())).append("\": ")
                            .append(entry.getValue());
                    if (cIdx < cTotal) {
                        sb.append(",");
                    }
                    sb.append("\n");
                }
                sb.append("      }\n");
            }

            sb.append("    }");
            if (i < ctx.jobs.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");

        return sb.toString();
    }

    private static void appendField(StringBuilder sb, String key, String val, int indent, boolean comma) {
        indent(sb, indent);
        sb.append("\"").append(escapeJson(key)).append("\": \"")
                .append(escapeJson(val != null ? val : "")).append("\"");
        if (comma) sb.append(",");
        sb.append("\n");
    }

    private static void appendNumber(StringBuilder sb, String key, long val, int indent, boolean comma) {
        indent(sb, indent);
        sb.append("\"").append(escapeJson(key)).append("\": ").append(val);
        if (comma) sb.append(",");
        sb.append("\n");
    }

    private static void appendBool(StringBuilder sb, String key, boolean val, int indent, boolean comma) {
        indent(sb, indent);
        sb.append("\"").append(escapeJson(key)).append("\": ").append(val);
        if (comma) sb.append(",");
        sb.append("\n");
    }

    private static void indent(StringBuilder sb, int count) {
        for (int i = 0; i < count; i++) {
            sb.append("  ");
        }
    }

    public static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (ch < ' ') {
                        String hex = Integer.toHexString(ch);
                        sb.append("\\u");
                        for (int k = 0; k < 4 - hex.length(); k++) sb.append('0');
                        sb.append(hex);
                    } else {
                        sb.append(ch);
                    }
            }
        }
        return sb.toString();
    }

    /**
     * Parses CLI arguments into PipelineContext.
     */
    public static int parseArgs(String[] args, PipelineContext ctx) {
        if (args.length == 0) {
            printUsage();
            return 1;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help":
                case "-h":
                    ctx.help = true;
                    return 0;
                case "--run-id":
                    if (++i < args.length) ctx.runId = args[i];
                    break;
                case "--input":
                    if (++i < args.length) ctx.inputPath = args[i];
                    break;
                case "--work-dir":
                case "--output":
                    if (++i < args.length) ctx.workDir = args[i];
                    break;
                case "--report-dir":
                    if (++i < args.length) ctx.reportDir = args[i];
                    break;
                case "--queue":
                    if (++i < args.length) ctx.queue = args[i];
                    break;
                case "--topn":
                    if (++i < args.length) {
                        try {
                            ctx.topN = Integer.parseInt(args[i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: Invalid --topn value: " + args[i]);
                            return 1;
                        }
                    }
                    break;
                case "--reducers-traffic":
                    if (++i < args.length) {
                        try {
                            ctx.reducersTraffic = Integer.parseInt(args[i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: Invalid --reducers-traffic value: " + args[i]);
                            return 1;
                        }
                    }
                    break;
                case "--reducers-session":
                    if (++i < args.length) {
                        try {
                            ctx.reducersSession = Integer.parseInt(args[i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: Invalid --reducers-session value: " + args[i]);
                            return 1;
                        }
                    }
                    break;
                case "--force":
                case "--overwrite":
                    ctx.force = true;
                    break;
                default:
                    System.err.println("Error: Unknown argument: " + arg);
                    printUsage();
                    return 1;
            }
        }

        if (ctx.inputPath == null || ctx.inputPath.trim().isEmpty()) {
            System.err.println("Error: Missing required argument: --input <path>");
            printUsage();
            return 1;
        }

        if (ctx.workDir == null || ctx.workDir.trim().isEmpty()) {
            System.err.println("Error: Missing required argument: --work-dir <path>");
            printUsage();
            return 1;
        }

        // Derive runId if not explicitly provided
        if (ctx.runId == null || ctx.runId.trim().isEmpty()) {
            Matcher m = Pattern.compile("dt=([0-9]{4}-[0-9]{2}-[0-9]{2})").matcher(ctx.inputPath);
            if (m.find()) {
                ctx.runId = "daily-" + m.group(1);
            } else {
                ctx.runId = "run-" + DateTimeFormatter.ISO_INSTANT.format(Instant.now()).replaceAll("[:.-]", "");
            }
        }

        return 0;
    }

    public static void printUsage() {
        System.out.println("Usage: hadoop jar clickstream-mr-1.0.jar edu.bigdata.clickstream.PipelineDriver [options]");
        System.out.println();
        System.out.println("Required Arguments:");
        System.out.println("  --input <path>               HDFS/Local path to raw clickstream CSV or partition");
        System.out.println("  --work-dir <path>            Pipeline output root directory for intermediate & final data");
        System.out.println();
        System.out.println("Optional Arguments:");
        System.out.println("  --run-id <id>                Identifier for run (default: inferred from dt=YYYY-MM-DD or timestamp)");
        System.out.println("  --queue <name>               YARN queue name (default: batch)");
        System.out.println("  --topn <N>                   Top-N items cutoff (default: 10)");
        System.out.println("  --reducers-traffic <N>       Number of reducers for J1 HourlyTraffic (default: 4)");
        System.out.println("  --reducers-session <N>       Number of reducers for J5 SessionFunnel (default: 8)");
        System.out.println("  --force, --overwrite         Overwrite existing work directory if it exists");
        System.out.println("  --report-dir <path>          Target directory recorded in metadata for M8 report export");
        System.out.println("  --help, -h                   Print this help message");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PipelineDriver(), args);
        System.exit(exitCode);
    }
}
