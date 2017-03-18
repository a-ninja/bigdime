package io.bigdime.handler.hive

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.TimeUnit

import com.google.common.base.Preconditions
import io.bigdime.alert.Logger.{ALERT_CAUSE, ALERT_SEVERITY, ALERT_TYPE}
import io.bigdime.alert.LoggerFactory
import io.bigdime.core.ActionEvent.Status
import io.bigdime.core.commons.{AdaptorLogger, DateNaturalLanguageExpressionParser, PropertyHelper}
import io.bigdime.core.config.AdaptorConfigConstants
import io.bigdime.core.handler.AbstractSourceHandler
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException
import io.bigdime.core.{ActionEvent, AdaptorConfigurationException, HandlerException, InvalidValueConfigurationException}
import io.bigdime.handler.StatusRecorder
import io.bigdime.libs.client.SwiftClient
import io.bigdime.libs.hdfs._
import io.bigdime.libs.hive.job.{HiveJobSpec, HiveJobStatus, JobStatusException, JobStatusFetcher}
import io.bigdime.util.Retry
import org.apache.hadoop.conf.Configuration
import org.javaswift.joss.exception.CommandException
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.springframework.beans.factory.annotation.{Autowired, Qualifier}
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

/**
  * Created by neejain on 3/6/17.
  */
object HiveJobHandler {
  val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[HiveJdbcReaderHandler]))

  val MILLS_IN_A_DAY: Long = 24 * 60 * 60 * 1000l
  val intervalInMins: Long = 24 * 60
  // default to a day
  val intervalInMillis: Long = intervalInMins * 60 * 1000
  val OUTPUT_DIRECTORY_DATE_FORMAT = "yyyy-MM-dd"
  val INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.hive.HiveJobHandler"
  val DEFAULT_GO_BACK_DAYS = 1
  val FORWARD_SLASH = "/"
  val DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS = TimeUnit.MINUTES.toSeconds(5)
  val DEFAULT_MAX_RETRIES = 5
  val DEFAULT_LATENCY = "0 hours"
  val DEFAULT_MIN_GO_BACK = "1 day"
}

@Component
@Scope("prototype")
class HiveJobHandler extends AbstractSourceHandler {

  import HiveJobHandler._

  private var outputDirectory: String = ""
  private val hiveConfigurations = new java.util.HashMap[String, String]
  @Autowired private val hiveJdbcConnectionFactory: HiveJdbcConnectionFactory = null
  @Autowired private val webHdfsReader: WebHdfsReader = null
  @Autowired
  @Qualifier("hiveJobStatusFether") private val hiveJobStatusFetcher: JobStatusFetcher[HiveJobSpec, HiveJobStatus] = null
  private var nextRunTimeRecordLoader: NextRunTimeRecordLoader[java.lang.Long, java.lang.Long] = _
  private var inputDescriptor: HiveReaderDescriptor = _
  private var hiveConfDateTime: Long = 0L
  private val handlerConfig = new HiveJdbcReaderHandlerConfig
  private val jobDtf = DateTimeFormat.forPattern("yyyyMMdd-HHmmss.SSS")
  private var hiveQueryDtf: DateTimeFormatter = _
  private val dateTimeZone: DateTimeZone = DateTimeZone.forID("America/Los_Angeles")
  private var hdfsOutputPathDtf: DateTimeFormatter = _
  private var sleepBetweenRetriesMillis: Long = 0L
  private var maxRetries: Int = 0
  private var conf: Configuration = _
  private var hiveConnectionParam: HiveConnectionParam = null
  //
  @Autowired private val context: ApplicationContext = null
  @Autowired private val swiftClient: SwiftClient = null

  @throws[AdaptorConfigurationException]
  override def build() {
    setHandlerPhase("building HiveJdbcReaderHandler")
    super.build()
    logger.info(getHandlerPhase, "properties={}", getPropertyMap)
    logger.info(getHandlerPhase, "webHdfsReader={}", webHdfsReader)
    var hiveConnectionParam: HiveConnectionParam = null
    /**
      * How many days should we go back to process the records. 0 means
      * process todays records, 1 means process yesterdays records
      */
    var goBackDays = DEFAULT_GO_BACK_DAYS
    val properties = getPropertyMap
    import scala.collection.JavaConversions._
    for (key <- properties.keySet) {
      logger.info(getHandlerPhase, "key=\"{}\" value=\"{}\"", key, getPropertyMap.get(key))
    }
    // sanity check for src-desc
    @SuppressWarnings(Array("unchecked")) val srcDescEntry = getPropertyMap.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC).asInstanceOf[java.util.Map.Entry[AnyRef, String]]
    if (srcDescEntry == null) throw new InvalidValueConfigurationException("src-desc can't be null")
    logger.info(getHandlerPhase, "src-desc-node-key=\"{}\" src-desc-node-value=\"{}\"", srcDescEntry.getKey, srcDescEntry.getValue)
    @SuppressWarnings(Array("unchecked")) val srcDescValueMap = srcDescEntry.getKey.asInstanceOf[java.util.Map[String, AnyRef]]
    val entityName = PropertyHelper.getStringProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.ENTITY_NAME)
    val hiveQuery = PropertyHelper.getStringProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_QUERY)
    goBackDays = PropertyHelper.getIntPropertyFromPropertiesOrSrcDesc(properties, srcDescValueMap, HiveJdbcReaderHandlerConstants.GO_BACK_DAYS, DEFAULT_GO_BACK_DAYS)
    var sleep = PropertyHelper.getLongProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.SLEEP_BETWEEN_RETRY_SECONDS, DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS)
    if (sleep == 0) sleep = DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS
    sleepBetweenRetriesMillis = TimeUnit.SECONDS.toMillis(sleep)
    maxRetries = PropertyHelper.getIntProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.MAX_RETRIES, DEFAULT_MAX_RETRIES)
    if (maxRetries < 0) maxRetries = 0
    logger.info(getHandlerPhase, "entityName=\"{}\" hiveQuery=\"{}\" goBackDays={} sleepBetweenRetriesMillis={} maxRetries={}", entityName, hiveQuery, goBackDays: java.lang.Integer, sleepBetweenRetriesMillis: java.lang.Long, maxRetries: java.lang.Integer)
    Preconditions.checkArgument(goBackDays >= 0, "{} has to be a non-negative value.", HiveJdbcReaderHandlerConstants.GO_BACK_DAYS: java.lang.String)
    val minGoBackExpression = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(properties, srcDescValueMap, HiveJdbcReaderHandlerConstants.MIN_GO_BACK, DEFAULT_MIN_GO_BACK)
    val minGoBackMillis = DateNaturalLanguageExpressionParser.toMillis(minGoBackExpression)
    Preconditions.checkArgument(goBackDays * MILLS_IN_A_DAY >= minGoBackMillis, "\"go-back-days\"({}) must be more than \"min-go-back\"({})", (goBackDays * MILLS_IN_A_DAY): java.lang.Long, minGoBackMillis: java.lang.Long)
    val latencyExpression = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(getPropertyMap, srcDescValueMap, HiveJdbcReaderHandlerConstants.LATENCY, DEFAULT_LATENCY)
    val latencyInMillis = DateNaturalLanguageExpressionParser.toMillis(latencyExpression)
    import scala.collection.JavaConversions._
    for (key <- srcDescValueMap.keySet) {
      logger.info(getHandlerPhase, "srcDesc-key=\"{}\" srcDesc-value=\"{}\"", key, srcDescValueMap.get(key))
    }
    setHiveConfigurations(srcDescValueMap)
    // Set JDBC params
    val jdbcUrl = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.JDBC_URL)
    val driverClassName = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.DRIVER_CLASS_NAME)
    val authChoice = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString)
    val authOption = HDFS_AUTH_OPTION.getByName(authChoice)
    val userName = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.HIVE_JDBC_USER_NAME)
    val password = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.HIVE_JDBC_SECRET)

    hiveConnectionParam = new HiveConnectionParam(authOption, driverClassName, jdbcUrl, userName, password, hiveConfigurations)

    val baseOutputDirectory = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.BASE_OUTPUT_DIRECTORY, "/")
    val outputDirectoryPattern = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.OUTPUT_DIRECTORY_DATE_FORMAT, OUTPUT_DIRECTORY_DATE_FORMAT)
    hdfsOutputPathDtf = DateTimeFormat.forPattern(outputDirectoryPattern)
    val hiveQueryDateFormat = PropertyHelper.getStringProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.HIVE_QUERY_DATE_FORMAT, "yyyy-MM-dd")
    hiveQueryDtf = DateTimeFormat.forPattern(hiveQueryDateFormat)
    val touchFile = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(properties, srcDescValueMap, HiveJdbcReaderHandlerConstants.TOUCH_FILE, null)
    logger.info(getHandlerPhase, "jdbcUrl=\"{}\" driverClassName=\"{}\" authChoice={} authOption={} userName=\"{}\" password=\"****\" baseOutputDirectory={} outputDirectoryPattern={} hiveQueryDateFormat={} touchFile={}", jdbcUrl, driverClassName, authChoice, authOption, userName, baseOutputDirectory, outputDirectoryPattern, hiveQueryDateFormat, touchFile)
    handlerConfig.setBaseOutputDirectory(baseOutputDirectory)
    handlerConfig.setEntityName(entityName)
    handlerConfig.setGoBackDays(goBackDays)
    handlerConfig.setHiveQuery(hiveQuery)
    handlerConfig.setMinGoBack(minGoBackMillis)
    handlerConfig.setLatency(latencyInMillis)
    handlerConfig.setTouchFile(touchFile)
    if (touchFile == null) nextRunTimeRecordLoader = new LatencyNextRunTimeRecordLoader(new TouchFileLookupConfig(handlerConfig), getPropertyMap)
    else nextRunTimeRecordLoader = new TouchFileNextRunTimeRecordLoader(webHdfsReader, new TouchFileLookupConfig(handlerConfig), getPropertyMap)
    logger.info(getHandlerPhase, "handlerConfig={}", handlerConfig)
  }

  @SuppressWarnings(Array("unchecked")) private def setHiveConfigurations(srcDescValueMap: java.util.Map[String, AnyRef]) {
    if (getPropertyMap.get(HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
      logger.info(getHandlerPhase, "found hive-conf in handler properties")
      hiveConfigurations.putAll(PropertyHelper.getMapProperty(getPropertyMap, HiveJdbcReaderHandlerConstants.HIVE_CONF))
      logger.info(getHandlerPhase, "hiveConfs from handler properties=\"{}\"", hiveConfigurations)
    }
    if (PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
      logger.info(getHandlerPhase, "found hive-conf in src-desc properties")
      hiveConfigurations.putAll(PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF))
    }
    logger.info(getHandlerPhase, "hiveConfs=\"{}\"", hiveConfigurations)
  }

  @throws[HandlerException]
  override def process: ActionEvent.Status = {
    setHandlerPhase("processing " + getName)
    incrementInvocationCount()
    logger.info(getHandlerPhase, "_messagge=\"entering process\" invocation_count={}", getInvocationCount: java.lang.Long)
    try {
      //      val hiveJobSpec = new HiveJobSpec("bigdime-temp-job", "bigdime-temp-dir")
      //      val hiveJobStatus = hiveJobStatusFetcher.getStatusForJobWithRetry(hiveJobSpec)
      //      logger.info(getHandlerPhase, "_messagge=\"retrieved status\" status={}", hiveJobStatus.getOverallStatus)

      val ts = List[Class[_ <: Throwable]](classOf[HandlerException], classOf[CommandException], classOf[java.sql.SQLException], classOf[IOException], classOf[InvocationTargetException], classOf[WebHdfsException])
      val SLEEP_BETWEEN_RETRIES = TimeUnit.MINUTES.toMillis(1)

      Retry(5, ts, SLEEP_BETWEEN_RETRIES)(() => {


        val webHdfsReaderForStream = context.getBean(classOf[WebHdfsReader])
        val inputStream = webHdfsReaderForStream.getInputStream("/sys/edw/dw_fdbk_mtrc_prfl_lftm/snapshot/2017/03/05/00/part-r-00000")
        logger.debug(getHandlerPhase, "got input stream")
        val targetPath = "2016-01-01__dw_fdbk_mtrc_prfl_lftm/00.txt"
        logger.info(getHandlerPhase, "webhdfs_path_to_process={} target_path={}", "/sys/edw/dw_fdbk_mtrc_prfl_lftm/snapshot/2017/03/05/00/part-r-00000", targetPath)
        val swiftObject = swiftClient.write(targetPath, inputStream)
        logger.info(getHandlerPhase, "wrote to swift from sink, and updated Runtime: targetPath={}", targetPath)
      }).get


      Status.READY
    } catch {
      case e: Exception => {
        throw new HandlerException("Unable to process", e)
      }
    }
  }

  @throws[RuntimeInfoStoreException]
  @throws[IOException]
  @throws[JobStatusException]
  protected def jobStartedSuccessfully(jobName: String, outputEvent: ActionEvent, ex: Exception): Boolean = {
    val hiveJobSpec = HiveJobSpec(jobName, inputDescriptor.getHiveConfDirectory)
    val hiveJobStatus = hiveJobStatusFetcher.getStatusForJobWithRetry(hiveJobSpec)

    val exMessage = if (ex == null) "" else ex.getMessage
    val recorder = StatusRecorder(this, getRuntimeInfoStore)

    Option(hiveJobStatus) match {
      case Some(_: HiveJobStatus) =>
        val runState = hiveJobStatus.getOverallStatus.getState
        runState match {
          case org.apache.hadoop.mapreduce.JobStatus.State.RUNNING | org.apache.hadoop.mapreduce.JobStatus.State.PREP =>
            val updatedRuntime = recorder.recordRunningStatus(outputEvent, inputDescriptor, hiveJobStatus.getOverallStatus)
            logger.warn(getHandlerPhase, "_message=\"exception in running the job, but job was created successfully\" updatedRuntime={} jobName={} runState={} errorMessage={}", updatedRuntime.toString, jobName.toString, runState.toString, exMessage.toString, ex)
            true
          case org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED =>
            val updatedRuntime = recorder.recordSuccessfulStatus(outputEvent, inputDescriptor, hiveJobStatus.getOverallStatus)
            logger.warn(getHandlerPhase, "_message=\"exception in running the job, but job was completed successfully\" updatedRuntime={} jobName={} runState={}", updatedRuntime.toString, jobName.toString, runState.toString, exMessage.toString, ex)
            true
          case _ =>
            val updatedRuntime = recorder.recordFailedStatus(outputEvent, inputDescriptor, hiveJobStatus.getOverallStatus)
            logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.NORMAL, ex, "_message=\"error in running the job\" updatedRuntime={} jobName={} runState={} error={}": java.lang.String, updatedRuntime.toString, jobName.toString, runState.toString, exMessage.toString)
            false
        }

      case None => false
    }
  }

  override def getEntityName: String = handlerConfig.getEntityName
}