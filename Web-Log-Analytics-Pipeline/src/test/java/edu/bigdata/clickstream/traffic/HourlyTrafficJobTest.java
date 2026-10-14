package edu.bigdata.clickstream.traffic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class HourlyTrafficJobTest {

    @Test
    @DisplayName("HourlyTrafficJob fails with exit code 1 when arguments are missing")
    void testMissingArguments() throws Exception {
        HourlyTrafficJob job = new HourlyTrafficJob();
        int exitCode = job.run(new String[]{});
        assertEquals(1, exitCode);

        int exitCodeOnlyInput = job.run(new String[]{"--input", "/some/path"});
        assertEquals(1, exitCodeOnlyInput);
    }

    @Test
    @DisplayName("HourlyTrafficJob fails with exit code 1 when output directory already exists")
    void testExistingOutputDirectoryFails(@TempDir File tempDir) throws Exception {
        HourlyTrafficJob job = new HourlyTrafficJob();
        Configuration conf = new Configuration();
        job.setConf(conf);

        File existingOut = new File(tempDir, "existing_out");
        assertTrue(existingOut.mkdirs());

        int exitCode = job.run(new String[]{
            "--input", new File(tempDir, "input.csv").getAbsolutePath(),
            "--output", existingOut.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Job must refuse to overwrite existing output directory");
    }

    @Test
    @DisplayName("Configuration keys can be set and read by job")
    void testConfigurationKeys() {
        Configuration conf = new Configuration();
        conf.setInt(HourlyTrafficJob.CONF_REDUCERS_TRAFFIC, 8);
        conf.set("mapreduce.job.queuename", "batch");

        assertEquals(8, conf.getInt(HourlyTrafficJob.CONF_REDUCERS_TRAFFIC, 4));
        assertEquals("batch", conf.get("mapreduce.job.queuename"));
    }
}
