package io.bigdime.handler

import io.bigdime.core.ActionEvent
import io.bigdime.core.commons.StringHelper
import io.bigdime.core.config.AdaptorConfig
import io.bigdime.core.constants.ActionEventHeaderConstants
import io.bigdime.core.handler.AbstractSourceHandler
import io.bigdime.core.runtimeinfo.{RuntimeInfo, RuntimeInfoStore, RuntimeInfoStoreException}
import io.bigdime.handler.hive.HiveReaderDescriptor

/**
  * Created by neejain on 3/6/17.
  */
case class StatusRecorder(handler: AbstractSourceHandler, runtimeInfoStore: RuntimeInfoStore[RuntimeInfo]) {
  private def populateHeaders(event: ActionEvent, jobStatus: org.apache.hadoop.mapred.JobStatus, statusHeaderName: String, status: String) {
    event.getHeaders.put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_ID, jobStatus.getJobID.toString)
    event.getHeaders.put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_START_TIME, "" + jobStatus.getStartTime)
    event.getHeaders.put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FINISH_TIME, "" + jobStatus.getFinishTime)
    event.getHeaders.put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FAILURE_INFO, jobStatus.getFailureInfo)
    event.getHeaders.put(statusHeaderName, status)
  }

  @throws[RuntimeInfoStoreException]
  def recordRunningStatus(event: ActionEvent, inputDescriptor: HiveReaderDescriptor, jobStatus: org.apache.hadoop.mapred.JobStatus) = {
    populateHeaders(event, jobStatus, ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_INTERIM_STATUS, org.apache.hadoop.mapred.JobStatus.getJobRunState(jobStatus.getRunState))
    updateRuntimeInfo(handler.getEntityName, inputDescriptor.getInputDescriptorString, RuntimeInfoStore.Status.STARTED, event.getHeaders)
  }

  @throws[RuntimeInfoStoreException]
  def recordSuccessfulStatus(event: ActionEvent, inputDescriptor: HiveReaderDescriptor, jobStatus: org.apache.hadoop.mapred.JobStatus) = {
    populateHeaders(event, jobStatus, ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_COMPLETION_STATUS, org.apache.hadoop.mapred.JobStatus.getJobRunState(jobStatus.getRunState))
    updateRuntimeInfo(handler.getEntityName, inputDescriptor.getInputDescriptorString, RuntimeInfoStore.Status.PENDING, event.getHeaders)
  }

  @throws[RuntimeInfoStoreException]
  def recordFailedStatus(event: ActionEvent, inputDescriptor: HiveReaderDescriptor, jobStatus: org.apache.hadoop.mapred.JobStatus) = {
    val rtiRecord: RuntimeInfo = runtimeInfoStore.get(AdaptorConfig.getInstance.getName, handler.getEntityName, inputDescriptor.getInputDescriptorString)
    var origJobId: String = rtiRecord.getProperties.get(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_ORIG_JOB_ID)
    if (StringHelper.isNotBlank(origJobId)) {
      origJobId = origJobId + "," + jobStatus.getJobID.toString
    }
    populateHeaders(event, jobStatus, ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_COMPLETION_STATUS, org.apache.hadoop.mapred.JobStatus.getJobRunState(jobStatus.getRunState))
    event.getHeaders.put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_ORIG_JOB_ID, origJobId)
    updateRuntimeInfo(handler.getEntityName, inputDescriptor.getInputDescriptorString, io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.FAILED, event.getHeaders)
  }

  @throws[RuntimeInfoStoreException]
  private def updateRuntimeInfo[T](entityName: String, inputDescriptor: String, status: RuntimeInfoStore.Status, properties: java.util.Map[String, String]): Boolean = {
    val startingRuntimeInfo = new RuntimeInfo
    startingRuntimeInfo.setAdaptorName(AdaptorConfig.getInstance.getName)
    startingRuntimeInfo.setEntityName(entityName)
    startingRuntimeInfo.setInputDescriptor(inputDescriptor)
    startingRuntimeInfo.setStatus(status)
    startingRuntimeInfo.setProperties(properties)
    runtimeInfoStore.put(startingRuntimeInfo)
  }
}
