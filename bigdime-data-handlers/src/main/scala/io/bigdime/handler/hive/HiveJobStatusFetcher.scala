package io.bigdime.handler.hive

import java.io.{FileInputStream, IOException}
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.commons.AdaptorLogger
import io.bigdime.handler.{JobStatusException, JobStatusFetcher}
import io.bigdime.libs.hdfs.WebHdfsException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobClient, JobConf, JobStatus}
import org.apache.hadoop.mapreduce.JobStatus.State
import org.apache.hadoop.security.UserGroupInformation
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import scala.collection.mutable.ListBuffer

/**
  * Created by neejain on 2/3/17.
  */
object HiveJobStatusFetcher {
  private val logger: AdaptorLogger = new AdaptorLogger(LoggerFactory.getLogger(classOf[HiveJobStatusFetcher]))
}

@Component("hiveJobStatusFether")
@Scope("prototype")
class HiveJobStatusFetcher extends JobStatusFetcher[HiveJobSpec, HiveJobStatus] {

  import HiveJobStatusFetcher.logger

  @Value("${hive.job.status.sleep.seconds:60}") private val sleepTimeBetweenStatusCallSeconds: Long = 0L
  private var sleepTimeBetweenStatusCall: Long = 0L
  @Value("${hive.job.status.max.wait.seconds:3600}") private val maxWaitSeconds = 0L
  @Value("${mapreduce.framework.name:yarn}") private val mapreduceFrameworkName: String = null
  @Value("${hadoop.security.authentication:kerberos}") private val hadoopSecurityAuthentication: String = null
  private var maxWait: Long = 0L
  private var jobClient: JobClient = null
  @Autowired private val jobClientFactory: JobClientFactory = null
  @Value("${yarn.site.xml.path}") private val yarnSiteXml: String = null
  @Value("${hive.jdbc.user.name}") private val userName: String = null
  @Value("${hive.jdbc.secret}") private val password: String = null
  @Autowired private val hiveJobOutputFileValidator: HiveJobOutputFileValidator = null
  private var conf: Configuration = null

  @PostConstruct
  @throws[Exception]
  def init() {
    logger.info("HiveJobStatusFetcher.PostConstruct", "yarnSiteXml={} mapreduceFrameworkName={} hadoopSecurityAuthentication={} userName={} secret={}, this={}", yarnSiteXml, mapreduceFrameworkName, hadoopSecurityAuthentication, userName, password, this)
    sleepTimeBetweenStatusCall = TimeUnit.SECONDS.toMillis(sleepTimeBetweenStatusCallSeconds)
    maxWait = TimeUnit.SECONDS.toMillis(maxWaitSeconds)
    conf = new Configuration
    conf.set("mapreduce.framework.name", mapreduceFrameworkName)
    conf.set("hadoop.security.authentication", hadoopSecurityAuthentication)
    if (yarnSiteXml != null) {
      val yarnSiteXmlInputStream = new FileInputStream(yarnSiteXml)
      conf.addResource(yarnSiteXmlInputStream)
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab(userName, password)
      jobClient = jobClientFactory.createJobClient(conf)
    }
    logger.info("HiveJobStatusFetcher.PostConstruct done", "yarnSiteXml={}, this={}", yarnSiteXml, this)
  }

  @throws[JobStatusException]
  def getStatusForAllJobs: Array[JobStatus] = {
    logger.info("getStatusForAllJobs", "yarnSiteXml={}, this={} mapreduceFrameworkName={} hadoopSecurityAuthentication={}", yarnSiteXml, this, mapreduceFrameworkName, hadoopSecurityAuthentication)
    try {
      jobClient.init(new JobConf(conf))
      val tempConf = new Configuration
      tempConf.set("mapreduce.framework.name", mapreduceFrameworkName)
      tempConf.set("hadoop.security.authentication", hadoopSecurityAuthentication)
      if (yarnSiteXml != null) {
        val yarnSiteXmlInputStream = new FileInputStream(yarnSiteXml)
        tempConf.addResource(yarnSiteXmlInputStream)
        UserGroupInformation.setConfiguration(tempConf)
        UserGroupInformation.loginUserFromKeytab(userName, password)
      }
      val jobStatus = jobClient.getAllJobs
      logger.debug("getStatusForAllJobs", "status={} length={}", jobStatus.toString, jobStatus.length)
      jobStatus

    } catch {
      case ex: IOException => {
        logger.info("getStatusForAllJobs", "_message=\"unable to get the job status\"", ex.toString)
        throw new JobStatusException("unable to get the job status:", ex)
      }
    } finally try
      jobClient.close()

    catch {
      case ex: IOException => {
        logger.warn("getStatusForAllJobs", "_message=\"exception while trying to close the jobClient...not a fatal error\"", ex)
      }
    }
  }

  @throws[JobStatusException]
  def getAllStatusesForJob(jobName: String): Array[JobStatus] = {
    val statuses = ListBuffer[JobStatus]()
    try {
      val jobStatus = getStatusForAllJobs
      logger.debug("getAllStatusesForJob", "status={} length={} jobName={}", jobStatus.toString, jobStatus.length, jobName)
      for (js <- jobStatus) {
        if (js.getJobName.contains(jobName)) statuses.append(js)
      }
    } catch {
      case ex: JobStatusException => {
        logger.info("getAllStatusesForJob", "_message=\"unable to get the job status\" jobName={} attempt={}", jobName, ex.toString)
        throw ex
      }
    }
    statuses.toArray
  }

  @throws[JobStatusException]
  def getStatusForJobWithRetry(jobSpec: HiveJobSpec): HiveJobStatus = {
    val jobName = jobSpec.getJobName
    val startTime = System.currentTimeMillis
    var endTime = startTime
    var hiveJobStatus: HiveJobStatus = null
    var attempt = 0
    var jobEx: JobStatusException = null
    do {
      attempt += 1
      try {
        jobEx = null
        hiveJobStatus = getStatusForJob(jobSpec)

      } catch {
        case ex: JobStatusException => {
          logger.info("getStatusForJobWithRetry", "_message=\"unable to get the job status\" jobName={} attempt={}", jobName, attempt, ex.toString)
          jobEx = ex
        }
      }
      if (hiveJobStatus == null) try {
        logger.info("getStatusForJobWithRetry", "_message=\"sleeping for {} ms before retry.\"  attempt={}", sleepTimeBetweenStatusCall, attempt)
        Thread.sleep(sleepTimeBetweenStatusCall)

      } catch {
        case ex: Exception => {
          logger.info("getStatusForJobWithRetry", "_message=\"Thread interrupted.\"  attempt={}", attempt, ex)
        }
      }
      endTime = System.currentTimeMillis
    } while (hiveJobStatus == null && (endTime - startTime) < maxWait)
    if (jobEx != null) throw jobEx
    hiveJobStatus
  }

  @throws[JobStatusException]
  def getStatusForJob(jobSpec: HiveJobSpec): HiveJobStatus = {

    val jobName = jobSpec.getJobName
    var overallJobStatus: JobStatus = null
    var newestJob: JobStatus = null
    var state: State = null
    val jobStatusList = new ListBuffer[JobStatus]
    var hiveJobStatus: HiveJobStatus = null
    var jobStatuses = getAllStatusesForJob(jobName)
    for (js <- jobStatuses) {
      logger.info("getStatusForJob", "jobId={} jobName={} runState={}", js.getJobID, js.getJobName, js.getRunState)
      jobStatusList.append(js)
      val stageState = js.getState
      if (newestJob == null) {
        newestJob = js
        state = stageState
        overallJobStatus = js
      }
      else if (newestJob.getStartTime < js.getStartTime) newestJob = js
      state match {
        case State.RUNNING =>
          if (stageState ne State.SUCCEEDED) {
            state = stageState
            overallJobStatus = js
          }
        case State.SUCCEEDED =>
          state = stageState
          overallJobStatus = js
        case State.FAILED =>
        case State.PREP =>
          if ((stageState eq State.KILLED) || (stageState eq State.FAILED)) {
            state = stageState
            overallJobStatus = js
          }
        case State.KILLED =>
      }
    }
    if (newestJob != null) {
      logger.info("getStatusForJob", "found a not null jobStatus. state={}", state)
      hiveJobStatus = HiveJobStatus(overallJobStatus, newestJob, jobStatusList.toList)
      if (hiveJobStatus.overallStatus.getState == State.SUCCEEDED)
        try {
          val validated = hiveJobOutputFileValidator.validateOutputFile(jobSpec.getOutputDirectoryPath)
          if (validated) logger.info("getStatusForJob", "_message=\"found a SUCCEEDED jobStatus and validated outputDirectory.\" jobName={} outputDirectoryPath={}", jobName, jobSpec.getOutputDirectoryPath)
          else {
            logger.info("getStatusForJob", "_message=\"found a SUCCEEDED jobStatus, but outputDirectory not found\" jobName={} outputDirectoryPath={}", jobName, jobSpec.getOutputDirectoryPath)
            hiveJobStatus = null
          }

        } catch {
          case ex: IOException => {
            logger.info("getStatusForJob", "_message=\"found a SUCCEEDED jobStatus, but unable to validate outputDirectory in hdfs.\" jobName={}", jobName, jobSpec.getOutputDirectoryPath, ex.toString)
            throw new JobStatusException("unable to validate outputDirectory in hdfs:" + jobSpec.getOutputDirectoryPath + ":", ex)
          }
          case ex: WebHdfsException => {
            logger.info("getStatusForJob", "_message=\"found a SUCCEEDED jobStatus, but unable to validate outputDirectory in hdfs.\" jobName={}", jobName, jobSpec.getOutputDirectoryPath, ex.toString)
            throw new JobStatusException("unable to validate outputDirectory in hdfs:" + jobSpec.getOutputDirectoryPath + ":", ex)
          }
        }
    }
    else logger.info("getStatusForJob", "found a null jobStatus")
    hiveJobStatus
  }
}
