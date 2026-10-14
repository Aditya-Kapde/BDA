package edu.bigdata.clickstream.topn;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class TopNJobTest {

    @Test
    @DisplayName("TopNJob fails with exit code 1 when arguments are missing or invalid")
    void testMissingArguments() throws Exception {
        TopNJob job = new TopNJob();
        assertEquals(1, job.run(new String[]{}));
        assertEquals(1, job.run(new String[]{"/some/input"}));
        assertEquals(1, job.run(new String[]{"/some/input", "/out", "extra_arg"}));
    }

    @Test
    @DisplayName("TopNJob fails with exit code 1 when output directory already exists")
    void testExistingOutputDirectoryFails(@TempDir File tempDir) throws Exception {
        TopNJob job = new TopNJob();
        Configuration conf = new Configuration();
        job.setConf(conf);

        File existingOut = new File(tempDir, "existing_topn_out");
        assertTrue(existingOut.mkdirs());

        int exitCode = job.run(new String[]{
            new File(tempDir, "j3_products").getAbsolutePath(),
            existingOut.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Job must refuse to overwrite existing output directory");
    }

    @Test
    @DisplayName("Configuration options: default queue 'batch', default TopN 10, and overrides")
    void testConfigurationDefaults() {
        Configuration conf = new Configuration();
        TopNJob.applyQueueConfiguration(conf);

        assertEquals("batch", conf.get(TopNJob.CONF_QUEUE_NAME));
        assertEquals(10, conf.getInt(TopNJob.CONF_TOPN, TopNJob.DEFAULT_TOPN));

        // Test overrides
        conf.setInt(TopNJob.CONF_TOPN, 25);
        conf.set(TopNJob.CONF_QUEUE_NAME, "interactive");
        TopNJob.applyQueueConfiguration(conf);

        assertEquals(25, conf.getInt(TopNJob.CONF_TOPN, TopNJob.DEFAULT_TOPN));
        assertEquals("interactive", conf.get(TopNJob.CONF_QUEUE_NAME));
    }
}
