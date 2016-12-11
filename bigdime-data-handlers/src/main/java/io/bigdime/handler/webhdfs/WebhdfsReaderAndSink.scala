package io.bigdime.handler.webhdfs

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Map.Entry
import java.util.concurrent.Executors
import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import io.bigdime.alert.Logger.ALERT_CAUSE
import io.bigdime.alert.Logger.ALERT_SEVERITY
import io.bigdime.alert.Logger.ALERT_TYPE
import io.bigdime.alert.LoggerFactory
import io.bigdime.core.ActionEvent
import io.bigdime.core.ActionEvent.Status
import io.bigdime.core.AdaptorConfigurationException
import io.bigdime.core.HandlerException
import io.bigdime.core.InvalidValueConfigurationException
import io.bigdime.core.commons.AdaptorLogger
import io.bigdime.core.commons.CollectionUtil
import io.bigdime.core.commons.PropertyHelper
import io.bigdime.core.commons.StringHelper
import io.bigdime.core.config.{AdaptorConfig, AdaptorConfigConstants}
import io.bigdime.core.constants.ActionEventHeaderConstants
import io.bigdime.core.handler.AbstractSourceHandler
import io.bigdime.core.handler.SimpleJournal
import io.bigdime.core.runtimeinfo.RuntimeInfo
import io.bigdime.core.runtimeinfo.RuntimeInfoStore
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException
import io.bigdime.handler.file.FileInputStreamReaderHandlerConstants
import io.bigdime.handler.swift.{SwiftClient, SwiftWriter, SwiftWriterHandlerConstants}
import io.bigdime.handler.webhdfs.WebHDFSReaderHandlerConfig.READ_HDFS_PATH_FROM
import io.bigdime.libs.hdfs.FileStatus
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION
import io.bigdime.libs.hdfs.WebHdfsException
import io.bigdime.libs.hdfs.WebHdfsReader

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import util.control.Breaks._

/**
  * Created by neejain on 12/9/16.
  */
@Component
@Scope("prototype")
class WebhdfsReaderAndSink extends AbstractSourceHandler {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[WebHDFSReaderHandler]))
  private val DEFAULT_BUFFER_SIZE = 1024 * 1024
  private val hdfsFileName: String = null
  private var entityName: String = null
  private var webHDFSPathParser: WebHDFSPathParser = null
  /**
    * CONFIG or HEADERS
    */
  @Autowired private val runtimeInfoStore: RuntimeInfoStore[RuntimeInfo] = null
  private var processingDirty = false
  //  private var inputDescriptor: WebHDFSInputDescriptor = _
  private val INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:"
  private val handlerConfig = new WebHDFSReaderHandlerConfig
  protected var inputFilePathPattern: String = null
  protected var outputFilePathPattern: String = null
  protected var inputPattern: Pattern = null
  @Autowired private val webHdfsReader: WebHdfsReader = null

  @Autowired private val swiftClient: SwiftClient = null

  @throws[AdaptorConfigurationException]
  override def build() {
    setHandlerPhase("building WebHDFSReaderHandler")
    super.build()
    try {
      var port = 0

      logger.info(getHandlerPhase, "building WebHDFSReaderHandler")
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
      webHDFSPathParser = WebHDFSPathParserFactory.getWebHDFSPathParser(readHdfsPathFrom)
      val hostNames = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.HOST_NAMES)
      port = PropertyHelper.getIntProperty(getPropertyMap, WebHDFSReaderHandlerConstants.PORT)
      val hdfsUser = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.HDFS_USER)
      val bufferSize = PropertyHelper.getIntProperty(getPropertyMap, FileInputStreamReaderHandlerConstants.BUFFER_SIZE, DEFAULT_BUFFER_SIZE)
      val waitForFileName = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.WAIT_FOR_FILE_NAME)
      val authChoice = PropertyHelper.getStringProperty(getPropertyMap, WebHDFSReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString)
      val authOption = HDFS_AUTH_OPTION.getByName(authChoice)
      //      logger.info(getHandlerPhase, "hostNames={} port={} hdfsUser={} hdfsFileName={} readHdfsPathFrom={}  authChoice={} authOption={} entityName={} webHDFSPathParser={} waitForFileName={}", hostNames, port, hdfsUser, hdfsFileName, readHdfsPathFrom, authChoice, authOption, entityName, webHDFSPathParser, waitForFileName)
      handlerConfig.setAuthOption(authOption)
      handlerConfig.setBufferSize(bufferSize)
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
      //      logger.info("swiftWriter=", Option(swiftClient.authUrl).getOrElse("null username").toString)
      //      logger.info("swiftWriter.tenantId=", Option(swiftClient.tenantId).getOrElse("null username").toString)
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
      //      logger.info(getHandlerPhase, "from header, entityName={} parentRuntimeId={}", entityName, parentRuntimeId)
    }
    else logger.info(getHandlerPhase, "from config, entityName={} ", entityName)
  }


  @throws[IOException]
  @throws[HandlerException]
  @throws[RuntimeInfoStoreException]
  override protected def doProcess: ActionEvent.Status = {
    if (recordList != null && recordList.nonEmpty) {
      processRecords(recordList)
      Status.READY
    } else
      Status.BACKOFF
  }

  @throws[HandlerException]
  def processWithRetry: ActionEvent.Status = {
    var success = false
    var attempt = 0
    val maxAttempts = 5
    var cause: Throwable = null
    do {
      setHandlerPhase("processing " + getName)
      incrementInvocationCount()
      //      logger.debug(getHandlerPhase, "_messagge=\"entering process\" invocation_count={}", getInvocationCount)
      try {
        attempt += 1
        init() // initialize cleanup records etc
        initDescriptor()
        if (isInputDescriptorNull) {
          logger.debug(getHandlerPhase, "returning BACKOFF")
          return io.bigdime.core.ActionEvent.Status.BACKOFF
        }
        val status = doProcess
        success = true
        return status
      }
      catch {
        case ex: HandlerException => {
          logger.warn(getHandlerPhase, "_message=\"file not found in hdfs\" error=\"{}\" cause=\"{}\"", ex.getMessage, ex.getCause.getMessage)
          if (ex.getCause != null && ex.getCause.getMessage != null && ex.getCause.getMessage == "Not Found") {
            logger.warn(getHandlerPhase, "_message=\"file not found in hdfs, returning backoff\" error={}", ex.getMessage)
            return Status.BACKOFF_NOW
          }
          else throw ex
        }
        case e: IOException => {
          //          logger.warn(getHandlerPhase, "_message=\"IOException received\" error={} attempt={} maxAttempts={}", e.getMessage, attempt, maxAttempts)
          cause = e
          // let it retry
        }
        case e: RuntimeInfoStoreException => {
          throw new HandlerException("Unable to process", e)
        }
        case e: Exception => {
          throw new HandlerException("Unable to process", e)
        }
      }
    } while (!success && attempt < maxAttempts)
    // If here, that means there was an IOException
    throw new HandlerException("Unable to process", cause)
  }

  @throws[HandlerException]
  override def process: ActionEvent.Status = {
    return processWithRetry
  }


  def processRecords(records: mutable.Buffer[RuntimeInfo]) = {
    val THREAD_POOL_SIZE = 10
    logger.info(getHandlerPhase, "processRecords")
    val futures = new ArrayBuffer[Future[Unit]]()
    Option(records) match {
      case null => 0
      case _ => {
        val batchSize = Math.min(records.size(), THREAD_POOL_SIZE)
        implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(THREAD_POOL_SIZE))
        var loopCount = 0
        breakable {
          records.foreach(_ => {
            val rec = records.remove(0)
            logger.info(getHandlerPhase, "rec={}", rec.getInputDescriptor)
            futures += Future {
              readWrite(rec)
//              val properties = new java.util.HashMap[String, String]
//              properties.put("handlerName", getHandlerClass)
//              updateRuntimeInfo(runtimeInfoStore, getEntityName, rec.getInputDescriptor, RuntimeInfoStore.Status.STARTED, properties)
//
//              var webHdfsPathToProcess: String = null
//              try {
//                val fullDescriptorStr = rec.getInputDescriptor
//                val inputDescriptor = new WebHDFSInputDescriptor
//                inputDescriptor.parseDescriptor(fullDescriptorStr)
//                webHdfsPathToProcess = inputDescriptor.getWebhdfsPath
//                val inputStream = webHdfsReader.getInputStream(webHdfsPathToProcess)
//                val targetPath = StringHelper.replaceTokens(webHdfsPathToProcess, outputFilePathPattern, inputPattern, properties)
//                logger.info(getHandlerPhase, "webHdfsPathToProcess={} targetPath={}", webHdfsPathToProcess, targetPath)
//                swiftClient.write(targetPath, inputStream)
//                logger.debug(getHandlerPhase, "current_file_path={}", inputDescriptor.getCurrentFilePath)
//              }
//              catch {
//                case e: Any => {
//                  rec.getProperties.put("error", e.getMessage)
//                  try {
//                    logger.debug(getHandlerPhase, "_message=\"deleting record from runtime info\" runtimeInfo={}", rec)
//                    runtimeInfoStore.delete(rec)
//                  }
//                  catch {
//                    case e1: RuntimeInfoStoreException => {
//                      logger.debug(getHandlerPhase, "_message=\"unable to update runtime info\" file_path={}", webHdfsPathToProcess)
//                    }
//                  }
//                  throw new HandlerException("unable to process:" + webHdfsPathToProcess + ", error=" + e.getMessage, e)
//                }
//              }
            }
            loopCount += 1
            if (loopCount >= batchSize) break
          }
          )
        }
        for (f <- futures) {
          f onComplete {
            case Success(x) => logger.info(getHandlerPhase, "Got the callback")
            case Failure(e) => logger.warn(getHandlerPhase, "future failed", e)
          }
        }

        for (f <- futures) {
          Await.result(f, Duration(600, "seconds"))
        }

      }
    }
  }

  private def readWrite(rec:RuntimeInfo) = {
    val properties = new java.util.HashMap[String, String]
    properties.put("handlerName", getHandlerClass)
    updateRuntimeInfo(runtimeInfoStore, getEntityName, rec.getInputDescriptor, RuntimeInfoStore.Status.STARTED, properties)

    var webHdfsPathToProcess: String = null
    try {
      val fullDescriptorStr = rec.getInputDescriptor
      val inputDescriptor = new WebHDFSInputDescriptor
      inputDescriptor.parseDescriptor(fullDescriptorStr)
      webHdfsPathToProcess = inputDescriptor.getWebhdfsPath
      val inputStream = webHdfsReader.getInputStream(webHdfsPathToProcess)
      val targetPath = StringHelper.replaceTokens(webHdfsPathToProcess, outputFilePathPattern, inputPattern, properties)
      logger.info(getHandlerPhase, "webHdfsPathToProcess={} targetPath={}", webHdfsPathToProcess, targetPath)
      swiftClient.write(targetPath, inputStream)
      logger.debug(getHandlerPhase, "current_file_path={}", inputDescriptor.getCurrentFilePath)
    }
    catch {
      case e: Any => {
        rec.getProperties.put("error", e.getMessage)
        try {
          logger.debug(getHandlerPhase, "_message=\"deleting record from runtime info\" runtimeInfo={}", rec)
          runtimeInfoStore.delete(rec)
        }
        catch {
          case e1: RuntimeInfoStoreException => {
            logger.debug(getHandlerPhase, "_message=\"unable to update runtime info\" file_path={}", webHdfsPathToProcess)
          }
        }
        throw new HandlerException("unable to process:" + webHdfsPathToProcess + ", error=" + e.getMessage, e)
      }
    }
  }
  private var recordList: mutable.Buffer[RuntimeInfo] = _

  @throws[RuntimeInfoStoreException]
  @throws[HandlerException]
  override def initDescriptorForNormal() {
    logger.info(getHandlerPhase, "initializing a clean record")
    processingDirty = false

    //process Manager.

    //queuedRecords
    //thread pool size =10.
    //submit the list of events

    //    import scala.collection.JavaConverters._
    //    import scala.collection.JavaConversions._

    if (recordList == null || recordList.isEmpty) {

      val queuedRecords = getAllRuntimeInfos(runtimeInfoStore, getEntityName, getInputDescriptorPrefix, RuntimeInfoStore.Status.QUEUED)
      if (queuedRecords == null || queuedRecords.isEmpty) {
        if (findAndAddRuntimeInfoRecords) recordList = getAllRuntimeInfos(runtimeInfoStore, getEntityName, getInputDescriptorPrefix, RuntimeInfoStore.Status.QUEUED).asScala
      }
    }
    //    val l = getAllRuntimeInfos(runtimeInfoStore, getEntityName, getInputDescriptorPrefix, RuntimeInfoStore.Status.QUEUED).asScala
    //    l
    //    l.remove()
    import scala.collection.JavaConverters._

    //    val l:scala.List[RuntimeInfo] = recordList.asScala.toList
    //    l match {
    //      case x::xs =>
    //    }
  }

  @throws[RuntimeInfoStoreException]
  @throws[HandlerException]
  override protected def findAndAddRuntimeInfoRecords: Boolean = {
    var recordsFound = false
    try {
      val availableHdfsDirectories = webHDFSPathParser.parse(getHdfsPath, getPropertyMap, getHandlerContext.getEventList, ActionEventHeaderConstants.HDFS_PATH)
      import scala.collection.JavaConversions._
      for (directoryPath <- availableHdfsDirectories) {
        try {
          recordsFound |= initializeRuntimeInfoRecords(directoryPath)
        }
        catch {
          case e: Any => {
            //            logger.warn(getHandlerPhase, "_message=\"could not initialized runtime info records\" recordsFound={}", recordsFound, e.getMessage)
            throw new HandlerException(e)
          }
        }
      }
      //      logger.info(getHandlerPhase, "_message=\"initialized runtime info records\" recordsFound={}", recordsFound)
      recordsFound
    }
    finally logger.debug(getHandlerPhase, "releasing webhdfs connection")
  }

  @throws[IOException]
  @throws[WebHdfsException]
  private def isReadyFilePresent(directoryPath: String) = {
    Option(waitForFileName) match {
      case Some(str)
      => webHdfsReader.getFileStatus(directoryPath, waitForFileName) != null
      case _ => true
    }
  }

  @throws[RuntimeInfoStoreException]
  protected def parentRuntimeRecordValid: Boolean = {
    val parentRuntimeId = getParentRuntimeIdFromHeader
    //    logger.info(getHandlerPhase, "parentRuntimeId={}", parentRuntimeId)
    if (parentRuntimeId != -1) {
      val rti = runtimeInfoStore.getById(Integer.valueOf(parentRuntimeId))
      logger.debug(getHandlerPhase, "runtimeRecordStatus={}", rti.getStatus)
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
      val parentRecordValid = parentRuntimeRecordValid
      if (parentRecordValid && isReadyFilePresent(directoryPath)) {
        val fileNames = webHdfsReader.list(directoryPath, false)
        import scala.collection.JavaConversions._
        for (fileName <- fileNames) {
          val properties = new java.util.HashMap[String, String]
          recordsFound = true
          properties.put(WebHDFSReaderHandlerConstants.HDFS_PATH, directoryPath)
          properties.put(WebHDFSReaderHandlerConstants.HDFS_FILE_NAME, fileName)
          // queueRuntimeInfo(runtimeInfoStore, entityName,
          // getInputDescriptorPrefix() + fileName, properties);
          val tempInputDescriptor = new WebHDFSInputDescriptor
          queueRuntimeInfo(runtimeInfoStore, entityName, tempInputDescriptor.createFullDescriptor(fileName), properties)
        }
      }
      //      else logger.info(getHandlerPhase, "_message=\"ready file is not present\" waitForFileName={} parentRecordValid={}", waitForFileName, parentRecordValid)
    }
    catch {
      case e: WebHdfsException => {
        if (e.getStatusCode == 404) logger.info(getHandlerPhase, "_message=\"path not found\" directoryPath={} error_message={}", directoryPath, e.getMessage)
        else if (e.getStatusCode == 401 || e.getStatusCode == 403) logger.warn(getHandlerPhase, "_message=\"auth error\" directoryPath={} error_message={}", directoryPath, e.getMessage)
      }
    }
    recordsFound
  }

  //  @throws[HandlerException]
  //  override protected def initRecordToProcess(runtimeInfo: RuntimeInfo) {
  //    var webHdfsPathToProcess: String = null
  //    try
  //      webHdfsReader.releaseWebHdfsForInputStream()
  //      val fullDescriptor = runtimeInfo.getInputDescriptor
  //      if (inputDescriptor == null) inputDescriptor = new WebHDFSInputDescriptor
  //      else if (inputDescriptor.getFileChannel != null) inputDescriptor.getFileChannel.close()
  //      inputDescriptor.parseDescriptor(fullDescriptor)
  //      webHdfsPathToProcess = inputDescriptor.getWebhdfsPath
  //      val inputStream = webHdfsReader.getInputStream(webHdfsPathToProcess)
  //      val currentFileStatus = getFileStatusFromWebhdfs(inputDescriptor.getWebhdfsPath)
  //      val fileChannel = Channels.newChannel(inputStream)
  //      inputDescriptor.setCurrentFileStatus(currentFileStatus)
  //      inputDescriptor.setFileChannel(fileChannel)
  //      logger.debug(getHandlerPhase, "current_file_path={} is_file_channel_open={}", inputDescriptor.getCurrentFilePath, fileChannel.isOpen)
  //
  //    catch {
  //      case e: Any => {
  //        runtimeInfo.getProperties.put("error", e.getMessage)
  //        try
  //          logger.debug(getHandlerPhase, "_message=\"deleting record from runtime info\" runtimeInfo={}", runtimeInfo)
  //          runtimeInfoStore.delete(runtimeInfo)
  //
  //        catch {
  //          case e1: RuntimeInfoStoreException => {
  //            logger.debug(getHandlerPhase, "_message=\"unable to update runtime info\" file_path={}", webHdfsPathToProcess)
  //          }
  //        }
  //        throw new HandlerException("unable to process:" + webHdfsPathToProcess + ", error=" + e.getMessage, e)
  //      }
  //    }
  //  }

  @throws[IOException]
  @throws[WebHdfsException]
  private def getFileStatusFromWebhdfs(hdfsFilePath: String) = {
    val fileStatus = webHdfsReader.getFileStatus(hdfsFilePath)
    fileStatus
  }

  @throws[HandlerException]
  private def totalReadFromJournal = getSimpleJournal.getTotalRead

  @throws[HandlerException]
  private def totalSizeFromJournal = getSimpleJournal.getTotalSize

  @throws[HandlerException]
  private def readAll = {
    //    logger.debug(getHandlerPhase, "total_read={} total_size={}", totalReadFromJournal, totalSizeFromJournal)
    totalReadFromJournal == totalSizeFromJournal
  }

  @throws[HandlerException]
  private def getSimpleJournal = getNonNullJournal(classOf[SimpleJournal])

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

  //  override protected def isInputDescriptorNull: Boolean = (inputDescriptor == null || inputDescriptor.getCurrentFilePath == null) || (inputDescriptor.getCurrentFileStatus.getLength eq 0)

  override protected def getInputDescriptorPrefix = INPUT_DESCRIPTOR_PREFIX

  //  protected def fileLength =
  //    if (isInputDescriptorNull) java.lang.Long.valueOf(0)
  //    else inputDescriptor.getCurrentFileStatus.getLength

  override def handleException() {
    try
      getSimpleJournal.reset()

    catch {
      case ex: HandlerException => {
        logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER, "_message=\"handler({}) is unable to handleException\" exception=\"{}\"", getName, ex.getMessage, ex)
      }
    }
  }

  override protected def setInputDescriptorToNull() {
    //    inputDescriptor = null
  }
}
