package io.bigdime.handler.hive

import java.util
import java.util.concurrent.TimeUnit

import io.bigdime.alert.Logger.{ALERT_CAUSE, ALERT_SEVERITY, ALERT_TYPE}
import io.bigdime.alert.LoggerFactory
import io.bigdime.core.ActionEvent
import io.bigdime.core.commons.{AdaptorLogger, CollectionUtil, StringHelper}
import io.bigdime.libs.hdfs.WebHdfsReader
import io.bigdime.util.LRUCache
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by neejain on 2/7/17.
  */
case class TouchFileLookupConfig(goBackDays: Int, minGoBack: Long, filePath: String, latency: Long) {
  def this(handlerConfig: HiveJdbcReaderHandlerConfig) = {
    this(handlerConfig.getGoBackDays, handlerConfig.getMinGoBack, handlerConfig.getTouchFile, handlerConfig.getLatency)
  }

  /**
    * Constructor to use when goBackDays is same as minGoBack.
    * @param goBackDays
    * @param filePath
    */
  def this(goBackDays: Int, filePath: String) = {
    this(goBackDays, goBackDays.toLong, filePath, 0l)
  }
}


trait NextRunTimeRecordLoader[I, O] {
  def getRecords(input: I): O

  val cache = LRUCache[String, Boolean](50)
  val logger: AdaptorLogger = new AdaptorLogger(LoggerFactory.getLogger(this.getClass))
  private val dateTimeZone: DateTimeZone = DateTimeZone.forID("America/Los_Angeles")

  protected def getAdjustedCurrentTime: Long = {
    getAdjustedCurrentTime(0)
  }

  protected def getAdjustedCurrentTime(latency: Long): Long = {
    val systemTime: Long = System.currentTimeMillis
    val nowPacific = DateTime.now(dateTimeZone).getMillis - latency
    logger.info("getAdjustedCurrentTime", "systemTime={} latency={} now={}", systemTime: java.lang.Long, latency: java.lang.Long, nowPacific: java.lang.Long)
    nowPacific
  }

  protected def getDateTimeInMillisForFirstRun(handlerConfig: TouchFileLookupConfig, now: Long): Long = {
    val nextRunDateTime: Long = now - handlerConfig.goBackDays * TimeUnit.DAYS.toMillis(1)
    logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run, set hiveConfDateTime done. no need to check for the touchFile, it may not even be present.\" hiveConfDateTime={}", nextRunDateTime: java.lang.Long)
    nextRunDateTime
  }

  protected def localPropertiesWithDateTokens(date: Long) = {
    val yearDtf = DateTimeFormat.forPattern("yyyy")
    val monthDtf = DateTimeFormat.forPattern("MM")
    val dateDtf = DateTimeFormat.forPattern("dd")
    val localProperties = new util.HashMap[String, String]
    localProperties.put("yyyy", yearDtf.print(date))
    localProperties.put("MM", monthDtf.print(date))
    localProperties.put("dd", dateDtf.print(date))
    localProperties
  }

}

case class TouchFileNextRunTimeRecordLoader(webHdfsReader: WebHdfsReader, handlerConfig: TouchFileLookupConfig, properties: java.util.Map[_ <: String, _]) extends NextRunTimeRecordLoader[java.lang.Long, java.lang.Long] {
  val intervalInMillis = TimeUnit.DAYS.toMillis(1)
  //  val cache = LRUCache[String, Boolean](50)

  override def getRecords(lastRunDateTime: java.lang.Long): java.lang.Long = {
    val now = getAdjustedCurrentTime
    // this is the first time
    if (lastRunDateTime == 0) {
      getDateTimeInMillisForFirstRun(now)
    }
    else getDateTimeInMillisForSubsequentRun(now, lastRunDateTime)
  }

  protected def getDateTimeInMillisForFirstRun(now: Long): Long = {
    val nextRunDateTime = now - handlerConfig.goBackDays * TimeUnit.DAYS.toMillis(1)
    logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run.\" attempted nextRunDateTime={}", nextRunDateTime.toString)
    var time = nextRunDateTime
    var tempNextRunDateTime = nextRunDateTime
    while (time == 0 && tempNextRunDateTime < now) {
      time = getDateTimeInMillis(now, 0, tempNextRunDateTime)
      logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run.\" tempNextRunDateTime={} output={}", tempNextRunDateTime.toString, time.toString)
      tempNextRunDateTime = tempNextRunDateTime + intervalInMillis
    }
    logger.info("getDateTimeInMillisForFirstRun", "_message=\"returning value\" nextRunDateTime={}", nextRunDateTime.toString)
    nextRunDateTime
  }

  private def getDateTimeInMillisForSubsequentRun(now: Long, lastRunDateTime: Long) = {
    val nextRunDateTime = lastRunDateTime + intervalInMillis
    var time: Long = 0
    var tempNextRunDateTime = nextRunDateTime
    while (time == 0 && getTouchFileDate(tempNextRunDateTime) < now) {
      time = getDateTimeInMillis(now, lastRunDateTime, tempNextRunDateTime)
      logger.info("getDateTimeInMillisForSubsequentRun", "_message=\"subsequent run.\" tempNextRunDateTime={} output={}", tempNextRunDateTime.toString, time.toString)
      tempNextRunDateTime = tempNextRunDateTime + intervalInMillis
    }
    if (time == 0) time
    else nextRunDateTime
  }


  private def getTouchFileDate(nextRunDateTime: Long) = nextRunDateTime + intervalInMillis


  private def getDateTimeInMillis(now: Long, lastRunDateTime: Long, nextRunDateTime: Long) = {
    logger.info("getDateTimeInMillis", "_message=\"will check touchfile.\" now={} nextRunDateTime={} intervalInMillis={}", now.toString, nextRunDateTime.toString, intervalInMillis.toString)
    val tokenizedPath = handlerConfig.filePath
    val tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath, "\\$\\{([\\w\\-]+)\\}+")
    val tokenSet = tokenToTokenName.keySet
    // if we are loading data for update_date>12/20, the touch file has the date of 12/21. so, we need to add the interval twice.
    val touchFileDate = getTouchFileDate(nextRunDateTime)
    val localProperties = localPropertiesWithDateTokens(touchFileDate)
    var detokString = tokenizedPath
    import scala.collection.JavaConversions._
    for (token <- tokenSet) {
      val tokenName = tokenToTokenName.get(token)
      if (localProperties != null && localProperties.get(tokenName) != null) detokString = detokString.replace(token, localProperties.get(tokenName).toString)
      if (properties != null && properties.get(tokenName) != null) detokString = detokString.replace(token, properties.get(tokenName).toString)
    }
    try {
      logger.info("checking touchfile", "file_to_check={}", detokString)
      if (webHdfsReader.getFileStatus(detokString, 2) != null) {
        logger.info("found touchfile", "file_to_check={}", detokString)
        nextRunDateTime
      }
      else {
        logger.info("nothing to do, touchfile not found", "now={} lastRunDateTime={} attempted_nextRunDateTime={} intervalInMillis={} file_to_check={}", now.toString, lastRunDateTime.toString, nextRunDateTime.toString, intervalInMillis.toString, detokString)
        0l
      }
    }
    catch {
      case e: Any => {
        //log a warning if we are mot able to find a touchfile for yesterday's records.
        //Dont worry about 2 days' old records, as the warning should've been logeed already for that.
        //Dont worry about todays' records, as the touch file might not be ready for it yet.
        if (now > touchFileDate && now - touchFileDate > intervalInMillis && now - touchFileDate < 2 * intervalInMillis) {
          if (!cache.contains(detokString)) {
            cache.put(detokString, true)
            logger.alert(ALERT_TYPE.INGESTION_DID_NOT_RUN, ALERT_CAUSE.MISSING_DATA, ALERT_SEVERITY.NORMAL, e, "file_path={} error_message={}", detokString, e.getMessage)
          }
        } else
          logger.info("getDateTimeInMillisForNextRun", "_message=\"file not found\" file_path=\"{}\" exception={}", detokString, e.getMessage)
        0l
      }
    }
  }
}

case class LatencyNextRunTimeRecordLoader(handlerConfig: TouchFileLookupConfig, properties: java.util.Map[_ <: String, _]) extends NextRunTimeRecordLoader[java.lang.Long, java.lang.Long] {
  private val intervalInMillis = TimeUnit.DAYS.toMillis(1)

  override def getRecords(lastRunDateTime: java.lang.Long): java.lang.Long = {
    val now = getAdjustedCurrentTime(handlerConfig.latency)
    if (lastRunDateTime == 0) {
      // this is the first time
      getDateTimeInMillisForFirstRun(handlerConfig, now)
    } else getDateTimeInMillisForSubsequentRun(handlerConfig, now, lastRunDateTime)
  }

  protected def getDateTimeInMillisForSubsequentRun(handlerConfig: TouchFileLookupConfig, now: Long, lastRunDateTime: Long): Long = {
    var nextRunDateTime = 0l
    if (now - lastRunDateTime > handlerConfig.minGoBack) {
      nextRunDateTime = lastRunDateTime + intervalInMillis
      logger.info("getDateTimeInMillisForNextRun", "_message=\"time to set hiveConfDateTime.\" now={} lastRunDateTime={} hiveConfDateTime={} intervalInMillis={}", now: java.lang.Long, lastRunDateTime: java.lang.Long, nextRunDateTime: java.lang.Long, intervalInMillis: java.lang.Long)
    }
    else logger.info("nothing to do", "now={} hiveConfDateTime={} intervalInMillis={} time_until_next_run={}", now: java.lang.Long, lastRunDateTime: java.lang.Long, intervalInMillis: java.lang.Long, (handlerConfig.minGoBack - (now - lastRunDateTime)): java.lang.Long)
    nextRunDateTime
  }

}

import scala.collection.JavaConversions._

case class WebhdfsDirectoryListHeaderBased(headerName: String) extends NextRunTimeRecordLoader[util.List[ActionEvent], util.List[String]] {

  override def getRecords(eventList: util.List[ActionEvent]): util.List[String] = {
    logger.debug("getting hdfsPath from header", "headerName={}", headerName)
    if (CollectionUtil.isEmpty(eventList)) {
      logger.debug("getting hdfsPath from header: no events found", "eventList={}", eventList)
      return null
    }
    var availableHdfsDirectories = List[String]()
    for (inputEvent <- eventList) {
      availableHdfsDirectories = inputEvent.getHeaders.get(headerName) :: availableHdfsDirectories
    }
    availableHdfsDirectories
  }
}


case class WebhdfsDirectoryListConfigBased(webHdfsReader: WebHdfsReader, handlerConfig: TouchFileLookupConfig, properties: util.Map[_ <: String, _]) extends NextRunTimeRecordLoader[util.List[ActionEvent], util.List[String]] {
  //  val cache = LRUCache[String, Boolean](50)

  override def getRecords(eventList: util.List[ActionEvent]): util.List[String] = {
    val tokenizedPath = handlerConfig.filePath
    logger.debug("detokenizing string", "tokenizedPath={} properties={}", tokenizedPath, properties)
    val yearDtf = DateTimeFormat.forPattern("yyyy")
    val monthDtf = DateTimeFormat.forPattern("MM")
    val dateDtf = DateTimeFormat.forPattern("dd")
    //    val hdfsPathList = new util.ArrayList[String]
    var hdfsPathList = new java.util.ArrayList[String]()
    val goBackDays = handlerConfig.goBackDays
    //PropertyHelper.getIntProperty(properties, WebHDFSReaderHandlerConstants.GO_BACK_DAYS, DEFAULT_GO_BACK_DAYS)
    val currentTime = System.currentTimeMillis
    val oldTime = currentTime - TimeUnit.DAYS.toMillis(goBackDays)
    val tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath, "\\$\\{([\\w\\-]+)\\}+")
    // System.out.println(tokenToTokenName);
    // {${dd}=dd, ${yyyy}=yyyy, ${MM}=MM}
    val localProperties = new util.HashMap[String, String]
    val tokenSet = tokenToTokenName.keySet
    var i = 0
    for (i <- 0 to goBackDays) {
      val time = oldTime + TimeUnit.DAYS.toMillis(i)
      logger.debug("detokenizing string:", "currentTime={} time={} tokenizedPath={}", currentTime: java.lang.Long, time: java.lang.Long, tokenizedPath)
      val localProperties = localPropertiesWithDateTokens(time)
      var detokString = tokenizedPath
      // say token = ${yyyy}, tokenName will be yyyy.
      import scala.collection.JavaConversions._
      for (token <- tokenSet) {
        val tokenName = tokenToTokenName.get(token)
        if (localProperties != null && localProperties.get(tokenName) != null) detokString = detokString.replace(token, localProperties.get(tokenName).toString)
        if (properties != null && properties.get(tokenName) != null) detokString = detokString.replace(token, properties.get(tokenName).toString)
      }
      logger.debug("detokenizing string: done", "tokenizedPath={} properties={} detokenizedString={}", tokenizedPath, properties, detokString)

      try {
        logger.info("checking hdfs file", "file_to_check={}", detokString)
        if (webHdfsReader.getFileStatus(detokString, 2) != null) {
          logger.info("found hdfs file", "file_to_check={}", detokString)
          hdfsPathList.add(detokString)
        }
      } catch {
        case e: Any => {
          //log a warning if we are not able to find a touchfile for yesterday's records.
          //if i == goBackDays-2, that means day before yesterday's file is not present yet
          if (goBackDays - i == 2) {
            if (!cache.contains(detokString)) {
              cache.put(detokString, true)
              logger.alert(ALERT_TYPE.INGESTION_DID_NOT_RUN, ALERT_CAUSE.MISSING_DATA, ALERT_SEVERITY.NORMAL, e, "file_path={} error_message={}", detokString, e.getMessage)
            } else {
              logger.warn("getDateTimeInMillisForNextRun", "_message=\"file not found\" file_path=\"{}\" exception={}", detokString, e.getMessage)
            }
          } else
            logger.info("getDateTimeInMillisForNextRun", "_message=\"file not found\" file_path=\"{}\" exception={}", detokString, e.getMessage)
        }
      }
    }
    hdfsPathList
  }
}
