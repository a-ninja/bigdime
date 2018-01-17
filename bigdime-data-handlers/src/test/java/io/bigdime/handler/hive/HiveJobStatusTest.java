package io.bigdime.handler.hive;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.libs.hive.job.HiveJobStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.testng.annotations.Test;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;

public class HiveJobStatusTest {

  @Test
  public void testGetters() {
    List<org.apache.hadoop.mapred.JobStatus> stageStatuses = List$.MODULE$.empty();
    JobStatus overallStatus = new JobStatus();
    JobStatus newestJobStatus = new JobStatus();
    HiveJobStatus hiveJobStatus = new HiveJobStatus(overallStatus, newestJobStatus, stageStatuses);

    GetterSetterTestHelper.doGetTest(hiveJobStatus, "overallStatus", overallStatus);
    GetterSetterTestHelper.doGetTest(hiveJobStatus, "newestJobStatus", newestJobStatus);
    GetterSetterTestHelper.doGetTest(hiveJobStatus, "stageStatuses", stageStatuses);
  }
}
