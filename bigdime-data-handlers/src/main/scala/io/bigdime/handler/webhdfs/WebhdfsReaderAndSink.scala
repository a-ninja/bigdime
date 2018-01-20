package io.bigdime.handler.webhdfs

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.{Executors, TimeUnit}
import java.util.regex.Pattern

import io.bigdime.alert.Logger.{ALERT_CAUSE, ALERT_SEVERITY, ALERT_TYPE}
import io.bigdime.alert.LoggerFactory
import io.bigdime.core.ActionEvent.Status
import io.bigdime.core._
import io.bigdime.core.commons._
import io.bigdime.core.config.AdaptorConfigConstants
import io.bigdime.core.constants.ActionEventHeaderConstants
import io.bigdime.core.handler.AbstractSourceHandler
import io.bigdime.core.runtimeinfo.{RuntimeInfo, RuntimeInfoStore, RuntimeInfoStoreException}
import io.bigdime.handler.hive.{NextRunTimeRecordLoader, TouchFileLookupConfig, WebhdfsDirectoryListConfigBased, WebhdfsDirectoryListHeaderBased}
import io.bigdime.handler.swift.SwiftWriterHandlerConstants
import io.bigdime.handler.webhdfs.WebHDFSReaderHandlerConfig.READ_HDFS_PATH_FROM
import io.bigdime.libs.client.SwiftClient
import io.bigdime.libs.hdfs.{HDFS_AUTH_OPTION, WebHdfsException, WebHdfsReader}
import io.bigdime.util.{LRUCache, Retry}
import org.apache.commons.lang3.StringUtils
import org.javaswift.joss.exception.CommandException
import org.javaswift.joss.model.StoredObject
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by neejain on 12/9/16.
  */
object WebhdfsReaderAndSink {
  private val logger = new AdaptorLoggerScala(LoggerFactory.getLogger(classOf[WebhdfsReaderAndSink]))
  private val INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.webhdfs.WebhdfsReaderAndSink,webhdfsPath:"
  private val THREAD_POOL_SIZE = 5
  private val executors = Executors.newFixedThreadPool(THREAD_POOL_SIZE)
  private val ec = ExecutionContext.fromExecutor(executors)

}

@Component
@Scope("prototype")
class WebhdfsReaderAndSink extends AbstractSourceHandler {

  import WebhdfsReaderAndSink._

  private val hdfsFileName: String = null
  private var entityName: String = _
  private var nextRunTimeRecordLoader: NextRunTimeRecordLoader[java.util.List[ActionEvent], java.util.List[String]] = _
  /**
    * CONFIG or HEADERS
    */
  @Autowired private val runtimeInfoStore: RuntimeInfoStore[RuntimeInfo] = null
  private var processingDirty = false

  private val handlerConfig = new WebHDFSReaderHandlerConfig
  protected var inputFilePathPattern: String = _
  protected var outputFilePathPattern: String = _
  protected var inputPattern: Pattern = _
  @Autowired private val webHdfsReader: WebHdfsReader = null
  @Autowired private val context: ApplicationContext = null

  @Autowired private val swiftClient: SwiftClient = null
  private var recordList: mutable.Buffer[RuntimeInfo] = _
  private var goBackDays = 3
  var cache: LRUCache[String, Boolean] = _
  val SLEEP_BETWEEN_RETRIES = TimeUnit.MINUTES.toMillis(1)

  @throws[AdaptorConfigurationException]
  override def build() {
    setHandlerPhase("building WebhdfsReaderAndSink")
    super.build()
    try {
      var port: Integer = 0

      logger.info(getHandlerPhase, "building WebhdfsReaderAndSink")
      val readHdfsPathFrom = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.READ_HDFS_PATH_FROM)
      if (StringUtils.equalsIgnoreCase(readHdfsPathFrom, "config")) {
        @SuppressWarnings(Array("unchecked")) val srcDescEntry = getPropertyMap.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC).asInstanceOf[java.util.Map.Entry[AnyRef, String]]
        logger.info(getHandlerPhase, "src-desc-node-key=\"{}\" src-desc-node-value=\"{}\"", srcDescEntry.getKey, srcDescEntry.getValue)
        @SuppressWarnings(Array("unchecked")) val srcDescValueMap = srcDescEntry.getKey.asInstanceOf[java.util.Map[String, AnyRef]]
        entityName = PropertyHelper.getStringProperty(srcDescValueMap, WebHDFSReaderHandlerConstants.ENTITY_NAME)
        val hdfsPath = PropertyHelper.getStringProperty(srcDescValueMap, WebHDFSReaderHandlerConstants.HDFS_PATH)

        getPropertyMap.put(WebHDFSReaderHandlerConstants.ENTITY_NAME, entityName)
        handlerConfig.setHdfsPath(hdfsPath)
      }
      val hostNames = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.HOST_NAMES)
      port = PropertyHelper.getIntProperty(getPropertyMap, WebHDFSReaderHandlerConstants.PORT)
      val hdfsUser = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.HDFS_USER)
      val waitForFileName = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.WAIT_FOR_FILE_NAME)
      val authChoice = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString)
      val authOption = HDFS_AUTH_OPTION.getByName(authChoice)

      //      parDoSize = PropertyHelper.getIntProperty(getPropertyMap, WebHDFSReaderAndSinkConstants.THREAD_POOL_SIZE, THREAD_POOL_SIZE)
      goBackDays = PropertyHelper.getIntProperty(getPropertyMap, WebHDFSReaderHandlerConstants.GO_BACK_DAYS, 3)
      cache = LRUCache[String, Boolean](goBackDays)
      logger.info(getHandlerPhase, "hostNames={} port={} hdfsUser={} hdfsFileName={} readHdfsPathFrom={}  authChoice={} authOption={} entityName={} waitForFileName={} goBackDays={}",
        hostNames, port, hdfsUser, hdfsFileName, readHdfsPathFrom, authChoice, authOption, entityName, waitForFileName, goBackDays: java.lang.Integer)
      handlerConfig.setAuthOption(authOption)
      handlerConfig.setEntityName(entityName)
      //      handlerConfig.setHdfsPath(hdfsPath)
      handlerConfig.setHdfsUser(hdfsUser)
      handlerConfig.setHostNames(hostNames)
      handlerConfig.setPort(port)
      handlerConfig.setReadHdfsPathFrom(readHdfsPathFrom)
      handlerConfig.setWaitForFileName(waitForFileName)
      inputFilePathPattern = PropertyHelper.getStringProperty(getPropertyMap, SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN)
      outputFilePathPattern = PropertyHelper.getStringProperty(getPropertyMap, SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN)
      inputPattern = Pattern.compile(inputFilePathPattern)

      if (readHdfsPathFrom == null) throw new InvalidValueConfigurationException("Invalid value for readHdfsPathFrom: \"" + readHdfsPathFrom + "\" not supported. Supported values are:" + READ_HDFS_PATH_FROM.values)
      logger.info("swiftWriter=", Option(swiftClient).getOrElse("null swiftWriter").toString)
      if (readHdfsPathFrom.equals("headers")) {
        nextRunTimeRecordLoader = WebhdfsDirectoryListHeaderBased(ActionEventHeaderConstants.HDFS_PATH)
      } else {
        nextRunTimeRecordLoader = WebhdfsDirectoryListConfigBased(webHdfsReader, new TouchFileLookupConfig(goBackDays, getHdfsPath), getPropertyMap)
      }

    }
    catch {
      case ex: Exception => {
        throw new AdaptorConfigurationException(ex)
      }
    }
  }

  /**
    * This method is executed when the handler is run the very first time. Use
    * it to initialize the connections, find the dirty records etc.
    *
    * @return true if the method completed with success, false otherwise.
    * @throws RuntimeInfoStoreException
    */
  @throws[RuntimeInfoStoreException]
  override protected def initClass() {
    if (isFirstRun) if (getReadHdfsPathFrom eq READ_HDFS_PATH_FROM.HEADERS) {
      entityName = getEntityNameFromHeader
      val parentRuntimeId = getParentRuntimeIdFromHeader
      logger.info(getHandlerPhase, "from header, entity_name={} parent_runtime_id={}", entityName, Integer.valueOf(parentRuntimeId))
    }
    else logger.info(getHandlerPhase, "from config, entity_name={} ", entityName)
  }


  @throws[IOException]
  @throws[HandlerException]
  @throws[RuntimeInfoStoreException]
  override protected def doProcess: ActionEvent.Status = {
    processRecords(recordList)
    if (readAll) {
      logger.debug(getHandlerPhase, "returning READY")
      Status.READY
    } else {
      logger.debug(getHandlerPhase, "returning CALLBACK")
      Status.CALLBACK
    }
  }

  @throws[HandlerException]
  override def process: ActionEvent.Status = {
    setHandlerPhase("processing WebhdfsReaderAndSink, entity_name=" + entityName)
    incrementInvocationCount
    init() // initialize cleanup records etc
    initDescriptor()
    if (isInputDescriptorNull) {
      logger.debug(getHandlerPhase, "returning BACKOFF")
      return io.bigdime.core.ActionEvent.Status.BACKOFF
    }
    doProcess
  }

  override protected def isInputDescriptorNull = recordList == null || recordList.isEmpty

  def processRecords(records: mutable.Buffer[RuntimeInfo]) = {

    logger.info(getHandlerPhase, "process_records.size={}", Integer.valueOf(records.size))
    val futures = new ArrayBuffer[Future[(WebHDFSInputDescriptor, StoredObject)]]()
    Option(records) match {
      case null => 0
      case _ => {
        val batchSize = Math.min(records.size(), THREAD_POOL_SIZE)
        //        val executors = Executors.newFixedThreadPool(batchSize)
        //        implicit val ec = ExecutionContext.fromExecutor(executors)
        implicit val executionContext = ec

        var loopCount = 0
        val iter = records.iterator
        while (iter.hasNext && loopCount < batchSize) {
          val rec = iter.next()
          iter.remove()
          logger.info(getHandlerPhase, "rec={}", rec.getInputDescriptor)
          futures += Future {
            readWrite(rec)
          }
          loopCount += 1
        }

        val eventList = new ListBuffer[ActionEvent]
        for (f <- futures) {
          f onComplete {
            case Success((inputDescriptor, soredObject)) => {
              logger.info(getHandlerPhase, "Got the callback...inputDescriptor.getFullDescriptor={}", inputDescriptor.getFullDescriptor)
              val innerEvent = new ActionEvent
              eventList += innerEvent
              logger.debug(getHandlerPhase, "setting header...{}", ActionEventHeaderConstants.SOURCE_FILE_NAME)
              innerEvent.getHeaders.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, inputDescriptor.getCurrentFilePath)

              logger.debug(getHandlerPhase, "setting header...{}", ActionEventHeaderConstants.INPUT_DESCRIPTOR)
              innerEvent.getHeaders.put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, inputDescriptor.getCurrentFilePath)
              logger.debug(getHandlerPhase, "setting header...{}", ActionEventHeaderConstants.FULL_DESCRIPTOR)
              innerEvent.getHeaders.put(ActionEventHeaderConstants.FULL_DESCRIPTOR, inputDescriptor.getFullDescriptor)
              logger.debug(getHandlerPhase, "setting header...{}", ActionEventHeaderConstants.ENTITY_NAME)
              innerEvent.getHeaders.put(ActionEventHeaderConstants.ENTITY_NAME, entityName)
              logger.info(getHandlerPhase, "submitting to channel={}", getOutputChannel.getName)
              processChannelSubmission(innerEvent)
            }
            case Failure(e) => logger.warn(getHandlerPhase, "future failed", e)
          }
        }

        for (f <- futures) {
          Await.result(f, Duration.Inf)
        }
        //        executors.shutdown()
      }
    }
  }

  private def readWrite(rec: RuntimeInfo): (WebHDFSInputDescriptor, StoredObject) = {
    val properties = new java.util.HashMap[String, String]
    properties.put("handlerName", getHandlerClass)
    properties.put(ActionEventHeaderConstants.ENTITY_NAME, entityName)
    updateRuntimeInfo(runtimeInfoStore, getEntityName, rec.getInputDescriptor, RuntimeInfoStore.Status.STARTED, properties)

    val ts = List[Class[_ <: Throwable]](classOf[HandlerException], classOf[CommandException], classOf[java.sql.SQLException], classOf[IOException], classOf[InvocationTargetException])

    Retry(5, ts, SLEEP_BETWEEN_RETRIES)(() => {
      val inputDescriptor = new WebHDFSInputDescriptor("handlerClass:io.bigdime.handler.webhdfs.WebhdfsReaderAndSink,webhdfsPath:")
      inputDescriptor.parseDescriptor(rec.getInputDescriptor)
      val webHdfsPathToProcess = inputDescriptor.getWebhdfsPath

      val webHdfsReaderForStream = context.getBean(classOf[WebHdfsReader])
      val inputStream = webHdfsReaderForStream.getInputStream(webHdfsPathToProcess)
      logger.debug(getHandlerPhase, "got input stream")
      val targetPath = StringHelper.replaceTokens(webHdfsPathToProcess, outputFilePathPattern, inputPattern, properties)
      logger.info(getHandlerPhase, "webhdfs_path_to_process={} target_path={}", webHdfsPathToProcess, targetPath)
      val swiftObject = swiftClient.write(targetPath, inputStream)
      updateRuntimeInfo(runtimeInfoStore, getEntityName, rec.getInputDescriptor, RuntimeInfoStore.Status.VALIDATED, properties)
      logger.info(getHandlerPhase, "wrote to swift from sink, and updated Runtime:webhdfs_path_to_process={} targetPath={}", webHdfsPathToProcess, targetPath)
      (inputDescriptor, swiftObject)
    }).get

  }

  //java.sql.SQLException
  @throws[RuntimeInfoStoreException]
  override def init() {
    if (isFirstRun) {
      initClass()
      dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, getEntityName, getInputDescriptorPrefix).asScala
      if (CollectionUtil.isNotEmpty(dirtyRecords)) {
        logger.warn(getHandlerPhase, "_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entity_name={}", getId, dirtyRecords.size.toString, getEntityName)
      } else
        logger.info(getHandlerPhase, "_message=\"no dirty records found\" handler_id={}", getId)
    }
  }

  @throws[HandlerException]
  @throws[RuntimeInfoStoreException]
  override protected def initDescriptor() {
    if (CollectionUtil.isNotEmpty(dirtyRecords))
      initDescriptorForCleanup()
    else
      initDescriptorForNormal()
  }

  @throws[HandlerException]
  override protected def initDescriptorForCleanup() {
    recordList = dirtyRecords.asScala
    processingDirty = true
  }

  @throws[RuntimeInfoStoreException]
  @throws[HandlerException]
  override def initDescriptorForNormal() {
    logger.info(getHandlerPhase, "initializing a clean record, entity_name={}", entityName)
    processingDirty = false

    if (recordList == null || recordList.isEmpty) {

      val queuedRecords = getAllRuntimeInfos(runtimeInfoStore, getEntityName, getInputDescriptorPrefix, RuntimeInfoStore.Status.QUEUED)
      if (queuedRecords == null || queuedRecords.isEmpty) {
        if (findAndAddRuntimeInfoRecords) recordList = getAllRuntimeInfos(runtimeInfoStore, getEntityName, getInputDescriptorPrefix, RuntimeInfoStore.Status.QUEUED).asScala
      } else recordList = queuedRecords.asScala
    }
  }

  @throws[RuntimeInfoStoreException]
  @throws[HandlerException]
  override protected def findAndAddRuntimeInfoRecords: Boolean = {
    var recordsFound: java.lang.Boolean = false
    try {
      val availableHdfsDirectories = nextRunTimeRecordLoader.getRecords(getHandlerContext.getEventList)
      for (directoryPath <- availableHdfsDirectories) {
        try {
          recordsFound |= initializeRuntimeInfoRecords(directoryPath)
        }
        catch {
          case e: Any => {
            logger.warn(getHandlerPhase, "_message=\"could not initialized runtime info records\" records_found={}", recordsFound: java.lang.Boolean, e.getMessage)
            throw new HandlerException(e)
          }
        }
      }
      logger.info(getHandlerPhase, "_message=\"initialized runtime info records\" records_found={}", recordsFound: java.lang.Boolean)
      recordsFound
    }
    finally logger.debug(getHandlerPhase, "releasing webhdfs connection")
  }

  @throws[IOException]
  @throws[WebHdfsException]
  private def isReadyFilePresent(directoryPath: String) = {
    logger.info(getHandlerPhase, "will check for ready file directoryPath=\"{}\" waitForFileName=\"{}\"", directoryPath, waitForFileName)
    Option(waitForFileName) match {
      case Some(str) =>
        val fileStatus = webHdfsReader.getFileStatus(directoryPath, waitForFileName)
        logger.info(getHandlerPhase, "after checking ready file directoryPath=\"{}\" waitForFileName=\"{}\", file_status_not_null={}", directoryPath, waitForFileName, (fileStatus != null).toString)
        fileStatus != null
      case _ => true
    }
  }

  @throws[RuntimeInfoStoreException]
  protected def parentRuntimeRecordValid: Boolean = {
    val parentRuntimeId: Integer = getParentRuntimeIdFromHeader
    logger.info(getHandlerPhase, "parent_runtime_id={}", parentRuntimeId)
    if (parentRuntimeId != -1) {
      val rti = runtimeInfoStore.getById(parentRuntimeId)
      logger.debug(getHandlerPhase, "runtime_record_status={}", rti.getStatus)
      return rti.getStatus eq RuntimeInfoStore.Status.PENDING
    }
    logger.debug(getHandlerPhase, "runtimeRecordStatus is valid, by default")
    true
  }

  @throws[RuntimeInfoStoreException]
  @throws[IOException]
  @throws[WebHdfsException]
  private def initializeRuntimeInfoRecords(directoryPath: String) = {
    var recordsFound = false
    try {
      val parentRecordValid: java.lang.Boolean = parentRuntimeRecordValid
      if (parentRecordValid && isReadyFilePresent(directoryPath)) {
        val fileNames = webHdfsReader.list(directoryPath, false)
        logger.info(getHandlerPhase, "will queue {} records", fileNames.size.toString)
        for (fileName <- fileNames) {
          val properties = new java.util.HashMap[String, String]
          recordsFound = true
          properties.put(WebHDFSReaderHandlerConstants.HDFS_PATH, directoryPath)
          properties.put(WebHDFSReaderHandlerConstants.HDFS_FILE_NAME, fileName)
          val tempInputDescriptor = new WebHDFSInputDescriptor("handlerClass:io.bigdime.handler.webhdfs.WebhdfsReaderAndSink,webhdfsPath:")
          queueRuntimeInfo(runtimeInfoStore, entityName, tempInputDescriptor.createFullDescriptor(fileName), properties)
        }
      }
      else logger.info(getHandlerPhase, "_message=\"ready file is not present\" wait_for_file_name={} parent_record_valid={}", waitForFileName, parentRecordValid)
    }
    catch {
      case e: WebHdfsException =>
        if (e.statusCode == 404) {
          if (!cache.contains(directoryPath)) {
            cache.put(directoryPath, true)
            logger.alert(ALERT_TYPE.INGESTION_DID_NOT_RUN, ALERT_CAUSE.MISSING_DATA, ALERT_SEVERITY.NORMAL, e, "handler_phase={} file_path={} error_message={}", getHandlerPhase, directoryPath, e.getMessage)
          }
        }
        else if (e.statusCode == 401 || e.statusCode == 403) logger.warn(getHandlerPhase, "_message=\"auth error\" directory_path={} error_message={}", directoryPath, e.getMessage)
    }
    recordsFound
  }

  @throws[IOException]
  @throws[WebHdfsException]
  private def getFileStatusFromWebhdfs(hdfsFilePath: String) = {
    val fileStatus = webHdfsReader.getFileStatus(hdfsFilePath)
    fileStatus
  }

  @throws[HandlerException]
  private def readAll = {
    recordList == null || recordList.isEmpty
  }

  def getHostNames: String = handlerConfig.getHostNames

  def getPort: Int = handlerConfig.getPort

  def getHdfsPath: String = handlerConfig.getHdfsPath

  override def getEntityName: String = entityName

  def setEntityName(entityName: String) {
    this.entityName = entityName
  }

  def getAuthOption: HDFS_AUTH_OPTION = handlerConfig.getAuthOption

  def getReadHdfsPathFrom: WebHDFSReaderHandlerConfig.READ_HDFS_PATH_FROM = handlerConfig.getReadHdfsPathFrom

  def getHdfsUser: String = handlerConfig.getHdfsUser

  def getBufferSize: Int = handlerConfig.getBufferSize

  private def waitForFileName = handlerConfig.getWaitForFileName

  override protected def getInputDescriptorPrefix = INPUT_DESCRIPTOR_PREFIX

  override def handleException() {
    //nothing to do
  }
}
