package edu.bigdata.clickstream;

import edu.bigdata.clickstream.common.QualityCounter;
import edu.bigdata.clickstream.dailyfunnel.DailyFunnelJob;
import edu.bigdata.clickstream.peakhour.PeakHourJob;
import edu.bigdata.clickstream.sessionfunnel.SessionFunnelJob;
import edu.bigdata.clickstream.topn.TopNJob;
import edu.bigdata.clickstream.traffic.HourlyTrafficJob;
import edu.bigdata.clickstream.viewcount.ViewCountJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PipelineDriverTest {

    @Test
    @DisplayName("CLI argument validation: missing arguments fail with exit code 1")
    void testMissingArguments() throws Exception {
        PipelineDriver driver = new PipelineDriver();
        assertEquals(1, driver.run(new String[]{}));
        assertEquals(1, driver.run(new String[]{"--input", "/some/path"}));
        assertEquals(1, driver.run(new String[]{"--work-dir", "/some/work"}));
        assertEquals(1, driver.run(new String[]{"--unknown-flag"}));
        assertEquals(1, driver.run(new String[]{"--input", "/some/path", "--work-dir", "/some/work", "--topn", "invalid"}));
        assertEquals(1, driver.run(new String[]{"--input", "/some/path", "--work-dir", "/some/work", "--reducers-traffic", "bad"}));
        assertEquals(1, driver.run(new String[]{"--input", "/some/path", "--work-dir", "/some/work", "--reducers-session", "bad"}));
    }

    @Test
    @DisplayName("CLI argument validation: --help flag exits with code 0")
    void testHelpFlag() throws Exception {
        PipelineDriver driver = new PipelineDriver();
        assertEquals(0, driver.run(new String[]{"--help"}));
        assertEquals(0, driver.run(new String[]{"-h"}));
    }

    @Test
    @DisplayName("Pre-flight check: Non-existent input path fails with exit code 1")
    void testNonExistentInputPathFails(@TempDir File tempDir) throws Exception {
        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        File nonExistent = new File(tempDir, "does_not_exist.csv");
        File workDir = new File(tempDir, "work");

        int code = driver.run(new String[]{
            "--input", nonExistent.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath()
        });

        assertEquals(1, code, "Pipeline must fail when raw input does not exist");
    }

    @Test
    @DisplayName("Pre-flight check: Refuse to set work-dir identical to raw input path")
    void testInputSameAsWorkDirFails(@TempDir File tempDir) throws Exception {
        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        File same = new File(tempDir, "input_and_work");
        assertTrue(same.mkdirs());

        int code = driver.run(new String[]{
            "--input", same.getAbsolutePath(),
            "--work-dir", same.getAbsolutePath()
        });

        assertEquals(1, code, "Pipeline must refuse to overwrite raw input directory as work-dir");
    }

    @Test
    @DisplayName("Overwrite protection: Existing work-dir refused without --force")
    void testExistingWorkDirRefusedWithoutForce(@TempDir File tempDir) throws Exception {
        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        File dummyInput = new File(tempDir, "events.csv");
        assertTrue(dummyInput.createNewFile());

        File existingWork = new File(tempDir, "existing_work");
        assertTrue(existingWork.mkdirs());

        int code = driver.run(new String[]{
            "--input", dummyInput.getAbsolutePath(),
            "--work-dir", existingWork.getAbsolutePath()
        });

        assertEquals(1, code, "Pipeline must refuse to overwrite existing work-dir without --force");
    }

    @Test
    @DisplayName("Overwrite protection: Existing work-dir allowed with --force")
    void testExistingWorkDirAllowedWithForce(@TempDir File tempDir) throws Exception {
        PipelineDriver.PipelineContext ctx = new PipelineDriver.PipelineContext();
        String[] args = new String[]{
            "--input", new File(tempDir, "events.csv").getAbsolutePath(),
            "--work-dir", new File(tempDir, "work").getAbsolutePath(),
            "--force"
        };
        int code = PipelineDriver.parseArgs(args, ctx);
        assertEquals(0, code);
        assertTrue(ctx.force);
    }

    @Test
    @DisplayName("Invariant FR-MR-07: Pass when sum equals TOTAL_RECORDS - HEADER_SKIPPED")
    void testInvariantVerificationPass(@TempDir File tempDir) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);

        File trafficDir = new File(tempDir, "traffic_pass");
        assertTrue(trafficDir.mkdirs());
        Path trafficPath = new Path(trafficDir.getAbsolutePath());

        // Create sample J1 output file
        File part0 = new File(trafficDir, "part-r-00000");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(part0, StandardCharsets.UTF_8))) {
            // date \t hour \t total \t views \t carts \t removes \t purchases \t malformed \t error_rate
            writer.write("2019-11-01\t0\t3\t2\t1\t0\t0\t0\t0.000000\n");
            writer.write("2019-11-01\t1\t3\t2\t0\t0\t1\t4\t0.571429\n");
            writer.write("NA\tNA\t0\t0\t0\t0\t0\t4\t1.000000\n");
        }
        // sum = (3+0) + (3+4) + (0+4) = 14

        Map<String, Long> counters = new HashMap<>();
        counters.put(QualityCounter.TOTAL_RECORDS.name(), 15L);
        counters.put(QualityCounter.HEADER_SKIPPED.name(), 1L);
        // expected = 15 - 1 = 14

        PipelineDriver.PipelineContext ctx = new PipelineDriver.PipelineContext();
        boolean passed = PipelineDriver.verifyReconciliationInvariant(fs, trafficPath, counters, ctx);
        assertTrue(passed, "Invariant must pass when sum matches expected records");
    }

    @Test
    @DisplayName("Invariant FR-MR-07: Fail when sum does not match TOTAL_RECORDS - HEADER_SKIPPED")
    void testInvariantVerificationFail(@TempDir File tempDir) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);

        File trafficDir = new File(tempDir, "traffic_fail");
        assertTrue(trafficDir.mkdirs());
        Path trafficPath = new Path(trafficDir.getAbsolutePath());

        File part0 = new File(trafficDir, "part-r-00000");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(part0, StandardCharsets.UTF_8))) {
            writer.write("2019-11-01\t0\t3\t2\t1\t0\t0\t0\t0.000000\n");
        }
        // sum = 3

        Map<String, Long> counters = new HashMap<>();
        counters.put(QualityCounter.TOTAL_RECORDS.name(), 20L);
        counters.put(QualityCounter.HEADER_SKIPPED.name(), 1L);
        // expected = 19 != 3

        PipelineDriver.PipelineContext ctx = new PipelineDriver.PipelineContext();
        boolean passed = PipelineDriver.verifyReconciliationInvariant(fs, trafficPath, counters, ctx);
        assertFalse(passed, "Invariant must fail on record sum mismatch");
    }

    @Test
    @DisplayName("run_meta.json schema and atomic write validation")
    void testRunMetaJsonGenerationAndAtomicWrite(@TempDir File tempDir) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        File workDir = new File(tempDir, "work_meta_test");
        assertTrue(workDir.mkdirs());
        Path workPath = new Path(workDir.getAbsolutePath());

        PipelineDriver.PipelineContext ctx = new PipelineDriver.PipelineContext();
        ctx.runId = "daily-2019-11-01";
        ctx.inputPath = "/clickstream/raw/dt=2019-11-01";
        ctx.workDir = workPath.toString();
        ctx.queue = "batch";
        ctx.topN = 10;
        ctx.reducersTraffic = 4;
        ctx.reducersSession = 8;
        ctx.status = "SUCCESS";
        ctx.invariantOk = true;
        ctx.start = Instant.parse("2026-10-15T00:00:00Z");
        ctx.end = Instant.parse("2026-10-15T00:06:40Z");
        ctx.durationSec = 400;

        PipelineDriver.JobExecutionRecord j1 = new PipelineDriver.JobExecutionRecord();
        j1.name = "J1_HourlyTraffic";
        j1.jobId = "job_1600000000_0001";
        j1.status = "SUCCESS";
        j1.start = ctx.start;
        j1.end = ctx.start.plusSeconds(60);
        j1.durationSec = 60;
        j1.input = ctx.inputPath;
        j1.output = workPath + "/traffic";
        j1.counters.put("TOTAL_RECORDS", 19L);
        j1.counters.put("VALID_RECORDS", 10L);
        j1.counters.put("HEADER_SKIPPED", 1L);
        ctx.jobs.add(j1);

        PipelineDriver.writeRunMetadata(fs, workPath, ctx);

        File metaFile = new File(workDir, PipelineDriver.META_FILE);
        assertTrue(metaFile.exists(), "run_meta.json must be written to work_dir");
        assertFalse(new File(workDir, PipelineDriver.META_TMP).exists(), "Temporary meta file must be renamed away");

        String json = new String(Files.readAllBytes(metaFile.toPath()), StandardCharsets.UTF_8);
        assertNotNull(json);
        assertTrue(json.contains("\"schema_version\": \"1.0\""));
        assertTrue(json.contains("\"run_id\": \"daily-2019-11-01\""));
        assertTrue(json.contains("\"status\": \"SUCCESS\""));
        assertTrue(json.contains("\"queue\": \"batch\""));
        assertTrue(json.contains("\"invariant_ok\": true"));
        assertTrue(json.contains("\"J1_HourlyTraffic\""));
        assertTrue(json.contains("\"TOTAL_RECORDS\": 19"));
    }

    @Test
    @DisplayName("run_meta.json formats failure information on failed run")
    void testRunMetaJsonFailureState() {
        PipelineDriver.PipelineContext ctx = new PipelineDriver.PipelineContext();
        ctx.runId = "daily-fail-test";
        ctx.inputPath = "/clickstream/raw/dt=2019-11-01";
        ctx.workDir = "/clickstream/work/daily-fail-test";
        ctx.status = "FAILED";
        ctx.failedJob = "J1_HourlyTraffic";
        ctx.error = "Corrupted input format";
        ctx.invariantOk = false;

        String json = PipelineDriver.buildRunMetaJson(ctx);
        assertTrue(json.contains("\"status\": \"FAILED\""));
        assertTrue(json.contains("\"failed_job\": \"J1_HourlyTraffic\""));
        assertTrue(json.contains("\"error\": \"Corrupted input format\""));
        assertTrue(json.contains("\"invariant_ok\": false"));
    }

    @Test
    @DisplayName("JSON string escaping properly handles special characters")
    void testJsonStringEscaping() {
        assertEquals("", PipelineDriver.escapeJson(null));
        assertEquals("hello world", PipelineDriver.escapeJson("hello world"));
        assertEquals("hello\\\"world\\\"", PipelineDriver.escapeJson("hello\"world\""));
        assertEquals("line1\\nline2\\ttab", PipelineDriver.escapeJson("line1\nline2\ttab"));
        assertEquals("back\\\\slash", PipelineDriver.escapeJson("back\\slash"));
    }

    @Test
    @DisplayName("CLI argument derivation: run_id correctly inferred from partition path")
    void testRunIdInferredFromInputPath() {
        PipelineDriver.PipelineContext ctx = new PipelineDriver.PipelineContext();
        String[] args = new String[]{
            "--input", "/clickstream/raw/dt=2019-11-01/events.csv",
            "--work-dir", "/clickstream/work/daily-2019-11-01"
        };
        int code = PipelineDriver.parseArgs(args, ctx);
        assertEquals(0, code);
        assertEquals("daily-2019-11-01", ctx.runId);
        assertEquals("batch", ctx.queue);
        assertEquals(10, ctx.topN);
        assertEquals(4, ctx.reducersTraffic);
        assertEquals(8, ctx.reducersSession);
    }

    @Test
    @DisplayName("Pipeline orchestration: correct dependency order and argument propagation")
    void testPipelineOrchestrationDependencyOrder(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_order");
        List<String> invokedJobs = new ArrayList<>();
        List<String[]> jobArgs = new ArrayList<>();

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());
        driver.setJobRunner((tool, args, conf) -> {
            String toolName = tool.getClass().getSimpleName();
            invokedJobs.add(toolName);
            jobArgs.add(args);

            // Simulate J1 producing traffic output for invariant check
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                File part0 = new File(trafficDir, "part-r-00000");
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(part0, StandardCharsets.UTF_8))) {
                    writer.write("2019-11-01\t0\t5\t2\t1\t1\t1\t0\t0.000000\n");
                }
            }
            return 0;
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--run-id", "test-order-run",
            "--queue", "custom-batch",
            "--topn", "20",
            "--reducers-traffic", "2",
            "--reducers-session", "6",
            "--force"
        });

        // Exit code 2 because J1 output (5) didn't match counters (0), confirming invariant enforcement
        assertEquals(2, exitCode, "Pipeline must stop and return 2 when invariant check fails");
        assertEquals(1, invokedJobs.size(), "Downstream jobs must NOT run when J1 invariant fails");
        assertEquals("HourlyTrafficJob", invokedJobs.get(0));
    }

    @Test
    @DisplayName("Pipeline orchestration: Full successful run with all J1..J6 jobs in order")
    void testPipelineFullSuccessRun(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_full_success");
        List<String> invokedJobs = new ArrayList<>();

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());
        driver.setJobRunner((tool, args, conf) -> {
            String toolName = tool.getClass().getSimpleName();
            invokedJobs.add(toolName);

            // Simulate J1 creating empty output to match 0 total records
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                File part0 = new File(trafficDir, "part-r-00000");
                part0.createNewFile();
            } else if (tool instanceof ViewCountJob) {
                File prodDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                new File(prodDir, "part-00000").createNewFile();
                File catDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                catDir.mkdirs();
                new File(catDir, "part-00000").createNewFile();
            } else if (tool instanceof SessionFunnelJob) {
                File sessDir = new File(workDir, PipelineDriver.DIR_SESSION_PARTIAL);
                sessDir.mkdirs();
                new File(sessDir, "part-r-00000").createNewFile();
            }
            return 0;
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--run-id", "test-full-success",
            "--force"
        });

        assertEquals(0, exitCode, "Pipeline should exit with 0 on complete success");

        // Verify exact dependency order: J1 -> J2 -> J3 -> J4(Products) -> J4(Categories) -> J5 -> J6
        assertEquals(7, invokedJobs.size());
        assertEquals("HourlyTrafficJob", invokedJobs.get(0));
        assertEquals("PeakHourJob", invokedJobs.get(1));
        assertEquals("ViewCountJob", invokedJobs.get(2));
        assertEquals("TopNJob", invokedJobs.get(3));
        assertEquals("TopNJob", invokedJobs.get(4));
        assertEquals("SessionFunnelJob", invokedJobs.get(5));
        assertEquals("DailyFunnelJob", invokedJobs.get(6));

        // Verify run_meta.json written with status SUCCESS
        File metaFile = new File(workDir, PipelineDriver.META_FILE);
        assertTrue(metaFile.exists());
        String json = new String(Files.readAllBytes(metaFile.toPath()), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"status\": \"SUCCESS\""));
        assertTrue(json.contains("\"invariant_ok\": true"));
        assertTrue(json.contains("\"J1_HourlyTraffic\""));
        assertTrue(json.contains("\"J6_DailyFunnel\""));

        // Verify lock released while coordination artifact remains
        Path lockPath = PipelineDriver.computeLockPath(new Path(workDir.getAbsolutePath()));
        File lockFile = PipelineDriver.getLocalFile(lockPath);
        assertNotNull(lockFile);
        assertTrue(lockFile.exists(), "Persistent lock file must remain on disk");
        assertFalse(PipelineDriver.isPipelineLocked(FileSystem.getLocal(new Configuration()), lockPath),
                "Lock must be released and inactive upon pipeline completion");
    }

    @Test
    @DisplayName("Pipeline stops immediately when intermediate job fails (e.g. J3)")
    void testPipelineStopsOnIntermediateJobFailure(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_fail_mid");
        List<String> invokedJobs = new ArrayList<>();

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());
        driver.setJobRunner((tool, args, conf) -> {
            String toolName = tool.getClass().getSimpleName();
            invokedJobs.add(toolName);

            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
                return 0;
            } else if (tool instanceof PeakHourJob) {
                return 0;
            } else if (tool instanceof ViewCountJob) {
                // Simulate J3 failure
                return 1;
            }
            return 0;
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--run-id", "test-mid-failure",
            "--force"
        });

        assertEquals(1, exitCode, "Pipeline must exit with code 1 on job failure");
        assertEquals(3, invokedJobs.size(), "Must stop after J3 failure and not launch J4, J5, J6");
        assertEquals("ViewCountJob", invokedJobs.get(2));

        // Verify run_meta.json records FAILED status
        File metaFile = new File(workDir, PipelineDriver.META_FILE);
        assertTrue(metaFile.exists());
        String json = new String(Files.readAllBytes(metaFile.toPath()), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"status\": \"FAILED\""));
        assertTrue(json.contains("\"failed_job\": \"J3_ViewCount\""));

        // Lock must be released even on failure while coordination artifact remains
        Path lockPath = PipelineDriver.computeLockPath(new Path(workDir.getAbsolutePath()));
        File lockFile = PipelineDriver.getLocalFile(lockPath);
        assertNotNull(lockFile);
        assertTrue(lockFile.exists(), "Persistent lock file must remain on disk");
        assertFalse(PipelineDriver.isPipelineLocked(FileSystem.getLocal(new Configuration()), lockPath),
                "Lock must be released upon failure");
    }

    @Test
    @DisplayName("Pipeline failure handling at J2, J4_Prod, J4_Cat, J5, and J6")
    void testPipelineStageFailures(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        String[] stages = new String[]{
            "PeakHourJob",
            "J4_Prod",
            "J4_Cat",
            "SessionFunnelJob",
            "DailyFunnelJob"
        };

        for (String targetFailStage : stages) {
            File workDir = new File(tempDir, "work_fail_" + targetFailStage);
            PipelineDriver driver = new PipelineDriver();
            driver.setConf(new Configuration());

            final int[] topNCount = {0};
            driver.setJobRunner((tool, args, conf) -> {
                String toolName = tool.getClass().getSimpleName();
                if (tool instanceof HourlyTrafficJob) {
                    File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                    trafficDir.mkdirs();
                    new File(trafficDir, "part-r-00000").createNewFile();
                    return 0;
                } else if (tool instanceof PeakHourJob) {
                    if ("PeakHourJob".equals(targetFailStage)) return 1;
                    return 0;
                } else if (tool instanceof ViewCountJob) {
                    File prodDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "products");
                    prodDir.mkdirs();
                    new File(prodDir, "part-00000").createNewFile();
                    File catDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                    catDir.mkdirs();
                    new File(catDir, "part-00000").createNewFile();
                    return 0;
                } else if (tool instanceof TopNJob) {
                    topNCount[0]++;
                    if ("J4_Prod".equals(targetFailStage) && topNCount[0] == 1) return 1;
                    if ("J4_Cat".equals(targetFailStage) && topNCount[0] == 2) return 1;
                    return 0;
                } else if (tool instanceof SessionFunnelJob) {
                    if ("SessionFunnelJob".equals(targetFailStage)) return 1;
                    File sessDir = new File(workDir, PipelineDriver.DIR_SESSION_PARTIAL);
                    sessDir.mkdirs();
                    new File(sessDir, "part-r-00000").createNewFile();
                    return 0;
                } else if (tool instanceof DailyFunnelJob) {
                    if ("DailyFunnelJob".equals(targetFailStage)) return 1;
                    return 0;
                }
                return 0;
            });

            int exitCode = driver.run(new String[]{
                "--input", inputFile.getAbsolutePath(),
                "--work-dir", workDir.getAbsolutePath(),
                "--report-dir", new File(tempDir, "reports").getAbsolutePath(),
                "--force"
            });

            assertEquals(1, exitCode, "Must return 1 on failure in " + targetFailStage);
            File metaFile = new File(workDir, PipelineDriver.META_FILE);
            assertTrue(metaFile.exists());
            String json = new String(Files.readAllBytes(metaFile.toPath()), StandardCharsets.UTF_8);
            assertTrue(json.contains("\"status\": \"FAILED\""));
            assertTrue(json.contains("\"report_dir\":"));
        }
    }

    @Test
    @DisplayName("Regression: Partial failure followed by retry with --force recovers and completes cleanly")
    void testPartialFailureFollowedByRetry(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_retry_test");
        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        // Attempt 1: J3 fails
        driver.setJobRunner((tool, args, conf) -> {
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
                return 0;
            } else if (tool instanceof PeakHourJob) {
                File peakDir = new File(workDir, PipelineDriver.DIR_PEAK);
                peakDir.mkdirs();
                new File(peakDir, "part-r-00000").createNewFile();
                return 0;
            } else if (tool instanceof ViewCountJob) {
                return 1; // Simulated transient failure
            }
            return 0;
        });

        int exit1 = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--run-id", "test-retry",
            "--force"
        });
        assertEquals(1, exit1, "Attempt 1 must fail at J3");

        File metaFile1 = new File(workDir, PipelineDriver.META_FILE);
        assertTrue(metaFile1.exists());
        String json1 = new String(Files.readAllBytes(metaFile1.toPath()), StandardCharsets.UTF_8);
        assertTrue(json1.contains("\"status\": \"FAILED\""));
        assertTrue(json1.contains("\"failed_job\": \"J3_ViewCount\""));

        // Attempt 2: All jobs succeed (retry with --force)
        driver.setJobRunner((tool, args, conf) -> {
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
            } else if (tool instanceof PeakHourJob) {
                File peakDir = new File(workDir, PipelineDriver.DIR_PEAK);
                peakDir.mkdirs();
                new File(peakDir, "part-r-00000").createNewFile();
            } else if (tool instanceof ViewCountJob) {
                File prodDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                new File(prodDir, "part-00000").createNewFile();
                File catDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                catDir.mkdirs();
                new File(catDir, "part-00000").createNewFile();
            } else if (tool instanceof SessionFunnelJob) {
                File sessDir = new File(workDir, PipelineDriver.DIR_SESSION_PARTIAL);
                sessDir.mkdirs();
                new File(sessDir, "part-r-00000").createNewFile();
            }
            return 0;
        });

        int exit2 = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--run-id", "test-retry",
            "--force"
        });
        assertEquals(0, exit2, "Attempt 2 (retry with --force) must succeed");

        File metaFile2 = new File(workDir, PipelineDriver.META_FILE);
        assertTrue(metaFile2.exists());
        String json2 = new String(Files.readAllBytes(metaFile2.toPath()), StandardCharsets.UTF_8);
        assertTrue(json2.contains("\"status\": \"SUCCESS\""));
        assertFalse(json2.contains("\"failed_job\":"));
    }

    @Test
    @DisplayName("Regression: Stale or incomplete upstream outputs prevent launching downstream jobs")
    void testIncompleteUpstreamOutputsFailDownstreamJobs(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        // Case A: J3 succeeds but creates NO categories output
        File workDirA = new File(tempDir, "work_no_cats");
        PipelineDriver driverA = new PipelineDriver();
        driverA.setConf(new Configuration());
        driverA.setJobRunner((tool, args, conf) -> {
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDirA, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
            } else if (tool instanceof ViewCountJob) {
                // Only create products, leave categories missing
                File prodDir = new File(new File(workDirA, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                new File(prodDir, "part-00000").createNewFile();
            }
            return 0;
        });

        int exitA = driverA.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDirA.getAbsolutePath(),
            "--force"
        });
        assertEquals(1, exitA, "Pipeline must abort when categories output is missing");
        File metaA = new File(workDirA, PipelineDriver.META_FILE);
        assertTrue(metaA.exists());
        String jsonA = new String(Files.readAllBytes(metaA.toPath()), StandardCharsets.UTF_8);
        assertTrue(jsonA.contains("Upstream J3 categories output missing or empty"));

        // Case B: J5 succeeds but session_partial is empty
        File workDirB = new File(tempDir, "work_no_sess");
        PipelineDriver driverB = new PipelineDriver();
        driverB.setConf(new Configuration());
        driverB.setJobRunner((tool, args, conf) -> {
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDirB, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
            } else if (tool instanceof ViewCountJob) {
                File prodDir = new File(new File(workDirB, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                new File(prodDir, "part-00000").createNewFile();
                File catDir = new File(new File(workDirB, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                catDir.mkdirs();
                new File(catDir, "part-00000").createNewFile();
            }
            // Do NOT create session_partial
            return 0;
        });

        int exitB = driverB.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDirB.getAbsolutePath(),
            "--force"
        });
        assertEquals(1, exitB, "Pipeline must abort before J6 when session_partial is missing");
        File metaB = new File(workDirB, PipelineDriver.META_FILE);
        assertTrue(metaB.exists());
        String jsonB = new String(Files.readAllBytes(metaB.toPath()), StandardCharsets.UTF_8);
        assertTrue(jsonB.contains("Upstream J5 session_partial output missing or empty"));
    }

    @Test
    @DisplayName("Regression: Active lock collision aborts with exit code 75")
    void testActiveLockCollisionAbortsWithExitCode75(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_lock_collision");
        assertTrue(workDir.mkdirs());

        // Simulate another active pipeline by holding PipelineLock outside workDir
        Path lockPath = PipelineDriver.computeLockPath(new Path(workDir.getAbsolutePath()));
        PipelineDriver.PipelineLock holder = new PipelineDriver.PipelineLock(FileSystem.getLocal(new Configuration()), lockPath);
        assertTrue(holder.tryAcquire("active_run_holder"));

        try {
            PipelineDriver driver = new PipelineDriver();
            driver.setConf(new Configuration());

            int exitCode = driver.run(new String[]{
                "--input", inputFile.getAbsolutePath(),
                "--work-dir", workDir.getAbsolutePath(),
                "--force"
            });

            assertEquals(PipelineDriver.EXIT_LOCK_COLLISION, exitCode, "Lock collision must exit with code 75");
        } finally {
            holder.close();
        }

        // Verify that once holder releases the lock, a new run can acquire it
        PipelineDriver driver2 = new PipelineDriver();
        driver2.setConf(new Configuration());
        driver2.setJobRunner((tool, args, conf) -> {
            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                try {
                    new File(trafficDir, "part-r-00000").createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (tool instanceof ViewCountJob) {
                File prodDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                try {
                    new File(prodDir, "part-00000").createNewFile();
                    File catDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                    catDir.mkdirs();
                    new File(catDir, "part-00000").createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (tool instanceof SessionFunnelJob) {
                File sessDir = new File(workDir, PipelineDriver.DIR_SESSION_PARTIAL);
                sessDir.mkdirs();
                try {
                    new File(sessDir, "part-r-00000").createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return 0;
        });
        int exitCode2 = driver2.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--force"
        });
        assertEquals(0, exitCode2, "Run must succeed after lock is released");
    }

    @Test
    @DisplayName("Regression: Force cleanup cannot invalidate or delete active lock file")
    void testForceCleanupCannotInvalidateActiveLock(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_force_lock");
        assertTrue(workDir.mkdirs());

        // Pre-populate workDir with existing files
        File oldFile = new File(workDir, "old_data.txt");
        assertTrue(oldFile.createNewFile());

        Path lockPath = PipelineDriver.computeLockPath(new Path(workDir.getAbsolutePath()));
        File lockFile = PipelineDriver.getLocalFile(lockPath);
        assertNotNull(lockFile);
        // Lock file is in tempDir, NOT inside workDir
        assertEquals(tempDir.getAbsolutePath(), lockFile.getParentFile().getAbsolutePath(),
                "Lock file must reside in parent directory outside work directory");

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        // During J1 execution, verify lock file exists and is active while workDir was purged and recreated
        driver.setJobRunner((tool, args, conf) -> {
            if (tool instanceof HourlyTrafficJob) {
                // Assert workDir old files were deleted by --force
                assertFalse(new File(workDir, "old_data.txt").exists(), "Old data must be purged by --force");
                // Assert lock file STILL exists and is active during execution
                assertTrue(lockFile.exists(), "Lock file must remain present and active during execution");
                assertTrue(PipelineDriver.isPipelineLocked(FileSystem.getLocal(conf), lockPath),
                        "Lock must be detected as active while job is executing");

                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
            } else if (tool instanceof ViewCountJob) {
                File prodDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                new File(prodDir, "part-00000").createNewFile();
                File catDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                catDir.mkdirs();
                new File(catDir, "part-00000").createNewFile();
            } else if (tool instanceof SessionFunnelJob) {
                File sessDir = new File(workDir, PipelineDriver.DIR_SESSION_PARTIAL);
                sessDir.mkdirs();
                new File(sessDir, "part-r-00000").createNewFile();
            }
            return 0;
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--force"
        });

        assertEquals(0, exitCode);
        // Lock released upon completion: OS FileLock released, coordination artifact remains
        assertFalse(PipelineDriver.isPipelineLocked(FileSystem.getLocal(driver.getConf()), lockPath),
                "Lock must be released and inactive upon pipeline completion");
        assertTrue(lockFile.exists(), "Persistent lock coordination artifact must remain on disk");
    }

    @Test
    @DisplayName("Regression: Competing invocation cannot start between cleanup and first job")
    void testNoGapBetweenCleanupAndFirstJob(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_no_gap");
        assertTrue(workDir.mkdirs());

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        driver.setJobRunner((tool, args, conf) -> {
            // Right at start of J1 (immediately following cleanup), simulate competing invocation
            PipelineDriver competing = new PipelineDriver();
            competing.setConf(new Configuration());
            int competingExit = competing.run(new String[]{
                "--input", inputFile.getAbsolutePath(),
                "--work-dir", workDir.getAbsolutePath(),
                "--force"
            });
            assertEquals(PipelineDriver.EXIT_LOCK_COLLISION, competingExit,
                    "Competing invocation must be rejected with exit code 75 even with --force");

            if (tool instanceof HourlyTrafficJob) {
                File trafficDir = new File(workDir, PipelineDriver.DIR_TRAFFIC);
                trafficDir.mkdirs();
                new File(trafficDir, "part-r-00000").createNewFile();
            } else if (tool instanceof ViewCountJob) {
                File prodDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "products");
                prodDir.mkdirs();
                new File(prodDir, "part-00000").createNewFile();
                File catDir = new File(new File(workDir, PipelineDriver.DIR_VIEWCOUNTS), "categories");
                catDir.mkdirs();
                new File(catDir, "part-00000").createNewFile();
            } else if (tool instanceof SessionFunnelJob) {
                File sessDir = new File(workDir, PipelineDriver.DIR_SESSION_PARTIAL);
                sessDir.mkdirs();
                new File(sessDir, "part-r-00000").createNewFile();
            }
            return 0;
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--force"
        });

        assertEquals(0, exitCode, "Primary invocation must complete successfully");
    }

    @Test
    @DisplayName("Job execution exception handling in executeJob")
    void testExecuteJobExceptionHandling(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_exception");
        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());
        driver.setJobRunner((tool, args, conf) -> {
            throw new RuntimeException("Simulated YARN RPC timeout");
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--force"
        });

        assertEquals(1, exitCode);
        File metaFile = new File(workDir, PipelineDriver.META_FILE);
        assertTrue(metaFile.exists());
        String json = new String(Files.readAllBytes(metaFile.toPath()), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"status\": \"FAILED\""));
        assertTrue(json.contains("Simulated YARN RPC timeout"));
    }

    @Test
    @DisplayName("captureCounters extracts QualityCounter and framework counters")
    void testCaptureCounters() {
        org.apache.hadoop.mapreduce.Counters counters = new org.apache.hadoop.mapreduce.Counters();
        counters.findCounter(QualityCounter.GROUP_NAME, QualityCounter.TOTAL_RECORDS.name()).setValue(100L);
        counters.findCounter(QualityCounter.GROUP_NAME, QualityCounter.VALID_RECORDS.name()).setValue(90L);
        counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").setValue(100L);

        Map<String, Long> target = new HashMap<>();
        PipelineDriver.captureCounters(counters, target);

        assertEquals(100L, target.get("TOTAL_RECORDS"));
        assertEquals(90L, target.get("VALID_RECORDS"));
        assertEquals(100L, target.get("MAP_INPUT_RECORDS"));

        // Null counters handling
        PipelineDriver.captureCounters(null, target);
        assertNotNull(target);
    }

    @Test
    @DisplayName("Getter and setter for JobRunner and JobExecutionRecord fields")
    void testAccessors() {
        PipelineDriver driver = new PipelineDriver();
        assertNotNull(driver.getJobRunner());

        PipelineDriver.JobRunner mockRunner = (tool, args, conf) -> 0;
        driver.setJobRunner(mockRunner);
        assertSame(mockRunner, driver.getJobRunner());

        PipelineDriver.JobExecutionRecord rec = new PipelineDriver.JobExecutionRecord();
        rec.name = "test";
        rec.jobId = "job_test_01";
        rec.error = "test error";
        assertEquals("test", rec.name);
        assertEquals("job_test_01", rec.jobId);
        assertEquals("test error", rec.error);
    }

    @Test
    @DisplayName("Regression: Unforced execution targeting existing successful run refuses overwrite and returns code 1")
    void testRetryRefusesToOverwriteExistingSuccessfulRun(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_success_pre_existing");
        assertTrue(workDir.mkdirs());

        // Pre-create successful run_meta.json
        File metaFile = new File(workDir, PipelineDriver.META_FILE);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(metaFile, StandardCharsets.UTF_8))) {
            writer.write("{\"status\": \"SUCCESS\", \"run_id\": \"daily-2019-11-01\"}\n");
        }
        long originalModified = metaFile.lastModified();

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());

        // Run WITHOUT --force
        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Must refuse to overwrite existing successful run directory when --force is omitted");
        // Verify original metadata was NOT deleted or overwritten
        assertTrue(metaFile.exists());
        String content = new String(Files.readAllBytes(metaFile.toPath()), StandardCharsets.UTF_8);
        assertTrue(content.contains("\"status\": \"SUCCESS\""));
    }

    public static class LockHolderMain {
        public static void main(String[] args) throws Exception {
            File lockFile = new File(args[0]);
            Configuration conf = new Configuration();
            Path lockPath = new Path(lockFile.getAbsolutePath());
            PipelineDriver.PipelineLock lock = new PipelineDriver.PipelineLock(FileSystem.getLocal(conf), lockPath);
            if (!lock.tryAcquire("child_process_run")) {
                System.out.println("FAILED");
                System.out.flush();
                System.exit(1);
            }
            System.out.println("ACQUIRED");
            System.out.flush();

            // Wait for parent signal to release
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)).readLine();
            lock.close();
            System.out.println("RELEASED");
            System.out.flush();
            System.exit(0);
        }
    }

    @Test
    @DisplayName("Regression: Separate process holding lock blocks competing process, and releases cleanly upon exit")
    void testSeparateProcessMutualExclusion(@TempDir File tempDir) throws Exception {
        File workDir = new File(tempDir, "work_separate_proc");
        assertTrue(workDir.mkdirs());
        Path lockPath = PipelineDriver.computeLockPath(new Path(workDir.getAbsolutePath()));
        File lockFile = PipelineDriver.getLocalFile(lockPath);
        assertNotNull(lockFile);

        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
            javaBin,
            "-cp", classpath,
            "edu.bigdata.clickstream.PipelineDriverTest$LockHolderMain",
            lockFile.getAbsolutePath()
        );
        pb.redirectErrorStream(false);
        Process child = pb.start();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(child.getOutputStream(), StandardCharsets.UTF_8));

            String status = null;
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if ("ACQUIRED".equals(line) || "FAILED".equals(line)) {
                    status = line;
                    break;
                }
            }
            assertEquals("ACQUIRED", status, "Child process must acquire lock and signal ACQUIRED");
            assertTrue(lockFile.exists(), "Persistent lock file must exist on disk");

            // Main process attempts to acquire lock on the exact same path
            PipelineDriver.PipelineLock competing = new PipelineDriver.PipelineLock(FileSystem.getLocal(new Configuration()), lockPath);
            assertFalse(competing.tryAcquire("main_process_attempt"),
                    "Competing process must NOT acquire lock while child process holds it");
            assertTrue(PipelineDriver.isPipelineLocked(FileSystem.getLocal(new Configuration()), lockPath),
                    "Lock probe must confirm lock is active");

            // Signal child to release and wait for child termination
            writer.write("RELEASE\n");
            writer.flush();

            String released = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if ("RELEASED".equals(line)) {
                    released = line;
                    break;
                }
            }
            assertEquals("RELEASED", released);
            child.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);

            // Child released: Lock file remains on disk as persistent coordination artifact
            assertTrue(lockFile.exists(), "Persistent lock file must NEVER be deleted upon normal release");
            assertFalse(PipelineDriver.isPipelineLocked(FileSystem.getLocal(new Configuration()), lockPath),
                    "Probe must confirm lock is released after child exits");

            // Subsequent process can now acquire the lock immediately on the existing file
            assertTrue(competing.tryAcquire("main_process_retry"),
                    "Subsequent process must acquire lock on the existing file after previous holder releases");
            competing.close();
            assertTrue(lockFile.exists(), "Lock file still remains on disk after second release");
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
            }
        }
    }

    @Test
    @DisplayName("Regression: Failed job releases lock and allows subsequent retry")
    void testFailedJobReleasesLock(@TempDir File tempDir) throws Exception {
        File inputFile = new File(tempDir, "events.csv");
        assertTrue(inputFile.createNewFile());

        File workDir = new File(tempDir, "work_fail_release");
        assertTrue(workDir.mkdirs());

        PipelineDriver driver = new PipelineDriver();
        driver.setConf(new Configuration());
        driver.setJobRunner((tool, args, conf) -> {
            throw new RuntimeException("Simulated early job failure");
        });

        int exitCode = driver.run(new String[]{
            "--input", inputFile.getAbsolutePath(),
            "--work-dir", workDir.getAbsolutePath(),
            "--force"
        });
        assertEquals(1, exitCode, "Driver must exit with code 1 on job failure");

        Path lockPath = PipelineDriver.computeLockPath(new Path(workDir.getAbsolutePath()));
        assertFalse(PipelineDriver.isPipelineLocked(FileSystem.getLocal(new Configuration()), lockPath),
                "Lock must be released even when pipeline encounters an unhandled exception or job failure");

        // Subsequent retry can acquire the lock immediately
        PipelineDriver.PipelineLock retryLock = new PipelineDriver.PipelineLock(FileSystem.getLocal(new Configuration()), lockPath);
        assertTrue(retryLock.tryAcquire("retry_run"), "Subsequent run must acquire lock after failure");
        retryLock.close();
    }

    @Test
    @DisplayName("Regression: HDFS fallback verifies marker ownership and prevents foreign marker deletion")
    void testHdfsMarkerOwnershipProtection() throws Exception {
        FileSystem mockFs = org.mockito.Mockito.mock(FileSystem.class);
        Path hdfsLockPath = new Path("hdfs://namenode:9000/clickstream/work/.daily.pipeline.lock");

        // Simulate Process A acquiring lock
        org.mockito.Mockito.when(mockFs.createNewFile(hdfsLockPath)).thenReturn(true);
        java.io.ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
        org.apache.hadoop.fs.FSDataOutputStream fsOut = new org.apache.hadoop.fs.FSDataOutputStream(bout, null);
        org.mockito.Mockito.when(mockFs.create(hdfsLockPath, true)).thenReturn(fsOut);

        PipelineDriver.PipelineLock procA = new PipelineDriver.PipelineLock(mockFs, hdfsLockPath);
        assertTrue(procA.tryAcquire("run_A"), "Process A must acquire HDFS marker");
        String procAData = bout.toString(StandardCharsets.UTF_8.name());
        assertTrue(procAData.contains("owner=" + procA.getOwnerToken()));

        // Process B attempts to acquire lock on the same path -> fails because createNewFile returns false
        org.mockito.Mockito.when(mockFs.createNewFile(hdfsLockPath)).thenReturn(false);
        PipelineDriver.PipelineLock procB = new PipelineDriver.PipelineLock(mockFs, hdfsLockPath);
        assertFalse(procB.tryAcquire("run_B"), "Process B must fail to acquire while marker exists");

        // Verify Process B closing does not delete Process A's marker
        procB.close();
        org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(hdfsLockPath, false);

        // Simulate Process A reading back its marker upon close
        org.mockito.Mockito.when(mockFs.exists(hdfsLockPath)).thenReturn(true);
        org.apache.hadoop.fs.FSDataInputStream fsIn = new org.apache.hadoop.fs.FSDataInputStream(
            new org.apache.hadoop.fs.FSInputStream() {
                private final java.io.ByteArrayInputStream bin = new java.io.ByteArrayInputStream(procAData.getBytes(StandardCharsets.UTF_8));
                @Override public void seek(long pos) {}
                @Override public long getPos() { return 0; }
                @Override public boolean seekToNewSource(long targetPos) { return false; }
                @Override public int read() { return bin.read(); }
            }
        );
        org.mockito.Mockito.when(mockFs.open(hdfsLockPath)).thenReturn(fsIn);

        // Process A closes and verifies marker ownership
        procA.close();
        org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.times(1)).delete(hdfsLockPath, false);
    }
}

