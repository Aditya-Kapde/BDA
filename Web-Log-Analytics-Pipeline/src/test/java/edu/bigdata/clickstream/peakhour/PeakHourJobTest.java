package edu.bigdata.clickstream.peakhour;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class PeakHourJobTest {

    @Test
    @DisplayName("PeakHourJob fails with exit code 1 when arguments are missing")
    void testMissingArguments() throws Exception {
        PeakHourJob job = new PeakHourJob();
        int exitCode = job.run(new String[]{});
        assertEquals(1, exitCode);

        int exitCodeOnlyInput = job.run(new String[]{"/some/input"});
        assertEquals(1, exitCodeOnlyInput);
    }

    @Test
    @DisplayName("PeakHourJob fails with exit code 1 when output directory already exists")
    void testExistingOutputDirectoryFails(@TempDir File tempDir) throws Exception {
        PeakHourJob job = new PeakHourJob();
        Configuration conf = new Configuration();
        job.setConf(conf);

        File existingOut = new File(tempDir, "existing_out");
        assertTrue(existingOut.mkdirs());

        int exitCode = job.run(new String[]{
            new File(tempDir, "j1_output").getAbsolutePath(),
            existingOut.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Job must refuse to overwrite existing output directory");
    }

    @Test
    @DisplayName("Queue name defaults to 'batch' and can be overridden via configuration")
    void testConfigurationQueue() {
        Configuration conf = new Configuration();
        PeakHourJob.applyQueueConfiguration(conf);
        assertEquals("batch", conf.get(PeakHourJob.CONF_QUEUE_NAME));

        conf.set(PeakHourJob.CONF_QUEUE_NAME, "analytics");
        PeakHourJob.applyQueueConfiguration(conf);
        assertEquals("analytics", conf.get(PeakHourJob.CONF_QUEUE_NAME));
    }
}
