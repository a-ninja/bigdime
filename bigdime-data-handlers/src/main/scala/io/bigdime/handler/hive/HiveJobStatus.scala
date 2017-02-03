package io.bigdime.handler.hive

import org.apache.hadoop.mapred.JobStatus

/**
  * Created by neejain on 2/3/17.
  */
case class HiveJobStatus(overallStatus: JobStatus, newestJobStatus: JobStatus, stageStatuses: List[JobStatus]) extends io.bigdime.handler.JobStatus {
  def getOverallStatus = overallStatus

  def getStageStatuses = if (stageStatuses == null) List[JobStatus]() else stageStatuses
}