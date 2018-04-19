package io.bigdime.libs.hive.job

/**
  * Created by neejain on 2/3/17.
  */
case class HiveJobStatus(overallStatus: org.apache.hadoop.mapred.JobStatus, newestJobStatus: org.apache.hadoop.mapred.JobStatus, stageStatuses: List[org.apache.hadoop.mapred.JobStatus]) {
  def getOverallStatus = overallStatus

  def getStageStatuses = if (stageStatuses == null) List[HiveJobStatus]() else stageStatuses
}