package io.bigdime.handler.hive;

import java.util.ArrayList;

import org.apache.hadoop.mapred.JobStatus;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;

public class HiveJobStatusTest {

    @Test
    public void testGetters() {
        HiveJobStatus hiveJobStatus = new HiveJobStatus(null, null, null);
        GetterSetterTestHelper.doTest(hiveJobStatus, "overallStatus", new JobStatus());
        GetterSetterTestHelper.doTest(hiveJobStatus, "newestJobStatus", new JobStatus());
        GetterSetterTestHelper.doTest(hiveJobStatus, "stageStatuses", new ArrayList<JobStatus>());
    }
}
