package edu.bigdata.clickstream.viewcount;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class ViewCountJobTest {

    @Test
    @DisplayName("ViewCountJob fails with exit code 1 when arguments are missing")
    void testMissingArguments() throws Exception {
        ViewCountJob job = new ViewCountJob();
        int exitCode = job.run(new String[]{});
        assertEquals(1, exitCode);

        int exitCodeOnlyInput = job.run(new String[]{"/some/input"});
        assertEquals(1, exitCodeOnlyInput);
    }

    @Test
    @DisplayName("ViewCountJob fails with exit code 1 when output directory already exists")
    void testExistingOutputDirectoryFails(@TempDir File tempDir) throws Exception {
        ViewCountJob job = new ViewCountJob();
        Configuration conf = new Configuration();
        job.setConf(conf);

        File existingOut = new File(tempDir, "existing_viewcount_out");
        assertTrue(existingOut.mkdirs());

        int exitCode = job.run(new String[]{
            new File(tempDir, "raw_clickstream.csv").getAbsolutePath(),
            existingOut.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Job must refuse to overwrite existing output directory");
    }

    @Test
    @DisplayName("Configuration options: default queue 'batch' and default 4 reducers")
    void testConfigurationDefaults() {
        Configuration conf = new Configuration();
        ViewCountJob.applyQueueConfiguration(conf);

        assertEquals("batch", conf.get(ViewCountJob.CONF_QUEUE_NAME));
        assertEquals(4, conf.getInt(ViewCountJob.CONF_REDUCERS_VIEWCOUNT, ViewCountJob.DEFAULT_REDUCERS));

        // Test overrides
        conf.setInt(ViewCountJob.CONF_REDUCERS_VIEWCOUNT, 8);
        conf.set(ViewCountJob.CONF_QUEUE_NAME, "interactive");
        ViewCountJob.applyQueueConfiguration(conf);
        assertEquals(8, conf.getInt(ViewCountJob.CONF_REDUCERS_VIEWCOUNT, ViewCountJob.DEFAULT_REDUCERS));
        assertEquals("interactive", conf.get(ViewCountJob.CONF_QUEUE_NAME));
    }
}
