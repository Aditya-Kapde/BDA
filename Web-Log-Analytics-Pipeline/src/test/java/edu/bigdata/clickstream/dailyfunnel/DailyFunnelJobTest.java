package edu.bigdata.clickstream.dailyfunnel;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class DailyFunnelJobTest {

    @Test
    @DisplayName("DailyFunnelJob fails with exit code 1 when arguments are missing")
    void testMissingArguments() throws Exception {
        DailyFunnelJob job = new DailyFunnelJob();
        int exitCode = job.run(new String[]{});
        assertEquals(1, exitCode);

        int exitCodeOnlyInput = job.run(new String[]{"/some/input"});
        assertEquals(1, exitCodeOnlyInput);
    }

    @Test
    @DisplayName("DailyFunnelJob fails with exit code 1 when output directory already exists")
    void testExistingOutputDirectoryFails(@TempDir File tempDir) throws Exception {
        DailyFunnelJob job = new DailyFunnelJob();
        Configuration conf = new Configuration();
        job.setConf(conf);

        File existingOut = new File(tempDir, "existing_funnel_out");
        assertTrue(existingOut.mkdirs());

        int exitCode = job.run(new String[]{
            new File(tempDir, "session_partial").getAbsolutePath(),
            existingOut.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Job must refuse to overwrite existing output directory");
    }

    @Test
    @DisplayName("Configuration options: default queue 'batch' and default 1 reducer")
    void testConfigurationDefaults() {
        Configuration conf = new Configuration();
        DailyFunnelJob.applyQueueConfiguration(conf);

        assertEquals("batch", conf.get(DailyFunnelJob.CONF_QUEUE_NAME));
        assertEquals(1, DailyFunnelJob.DEFAULT_REDUCERS);

        // Test overrides
        conf.set(DailyFunnelJob.CONF_QUEUE_NAME, "interactive");
        DailyFunnelJob.applyQueueConfiguration(conf);
        assertEquals("interactive", conf.get(DailyFunnelJob.CONF_QUEUE_NAME));
    }
}
