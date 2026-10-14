package edu.bigdata.clickstream.sessionfunnel;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class SessionFunnelJobTest {

    @Test
    @DisplayName("SessionFunnelJob fails with exit code 1 when arguments are missing")
    void testMissingArguments() throws Exception {
        SessionFunnelJob job = new SessionFunnelJob();
        int exitCode = job.run(new String[]{});
        assertEquals(1, exitCode);

        int exitCodeOnlyInput = job.run(new String[]{"/some/input"});
        assertEquals(1, exitCodeOnlyInput);
    }

    @Test
    @DisplayName("SessionFunnelJob fails with exit code 1 when output directory already exists")
    void testExistingOutputDirectoryFails(@TempDir File tempDir) throws Exception {
        SessionFunnelJob job = new SessionFunnelJob();
        Configuration conf = new Configuration();
        job.setConf(conf);

        File existingOut = new File(tempDir, "existing_session_out");
        assertTrue(existingOut.mkdirs());

        int exitCode = job.run(new String[]{
            new File(tempDir, "raw_clickstream.csv").getAbsolutePath(),
            existingOut.getAbsolutePath()
        });

        assertEquals(1, exitCode, "Job must refuse to overwrite existing output directory");
    }

    @Test
    @DisplayName("Configuration options: default queue 'batch' and default 8 reducers")
    void testConfigurationDefaults() {
        Configuration conf = new Configuration();
        SessionFunnelJob.applyQueueConfiguration(conf);

        assertEquals("batch", conf.get(SessionFunnelJob.CONF_QUEUE_NAME));
        assertEquals(8, conf.getInt(SessionFunnelJob.CONF_REDUCERS_SESSION, SessionFunnelJob.DEFAULT_REDUCERS_SESSION));

        // Test overrides
        conf.setInt(SessionFunnelJob.CONF_REDUCERS_SESSION, 16);
        conf.set(SessionFunnelJob.CONF_QUEUE_NAME, "interactive");
        SessionFunnelJob.applyQueueConfiguration(conf);
        assertEquals(16, conf.getInt(SessionFunnelJob.CONF_REDUCERS_SESSION, SessionFunnelJob.DEFAULT_REDUCERS_SESSION));
        assertEquals("interactive", conf.get(SessionFunnelJob.CONF_QUEUE_NAME));
    }
}
