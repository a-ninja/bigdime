package io.bigdime.handler.hive

import java.util
import java.util.concurrent.TimeUnit

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.commons.{AdaptorLogger, StringHelper}
import io.bigdime.libs.hdfs.WebHdfsReader
import org.joda.time.format.DateTimeFormat

/**
  * Created by neejain on 1/31/17.
  */
object TouchFileChecker {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[TouchFileChecker]))
}

case class TouchFileChecker(webHdfsReader: WebHdfsReader, intervalInMillis: Long) extends AbstractNextRunChecker {

  import TouchFileChecker.logger

  def this(webHdfsReader: WebHdfsReader) {
    this(webHdfsReader, TimeUnit.DAYS.toMillis(1))
  }

  def getDateTimeInMillisForNextRun(lastRunDateTime: Long, handlerConfig: HiveJdbcReaderHandlerConfig, properties: util.Map[_ <: String, _]): Long = {
    val now = getAdjustedCurrentTime
    // this is the first time
    if (lastRunDateTime == 0) {
      getDateTimeInMillisForFirstRun(handlerConfig, now, properties)
    }
    else getDateTimeInMillisForSubsequentRun(handlerConfig, now, lastRunDateTime, properties)
  }

  /*
	 * Set the nextRunDateTime to currentTime - goBackDays. Say this time is T1.
	 * If any folder is found for T1 or after, return the time as T1.
	 */
  protected def getDateTimeInMillisForFirstRun(handlerConfig: HiveJdbcReaderHandlerConfig, now: Long, properties: util.Map[_ <: String, _]): Long = {
    val nextRunDateTime = now - handlerConfig.getGoBackDays * TimeUnit.DAYS.toMillis(1)
    logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run.\" attempted nextRunDateTime={}", nextRunDateTime.toString)
    var time = nextRunDateTime
    var tempNextRunDateTime = nextRunDateTime
    while (time == 0 && tempNextRunDateTime < now) {
      time = getDateTimeInMillis(handlerConfig, now, 0, tempNextRunDateTime, properties)
      logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run.\" tempNextRunDateTime={} output={}", tempNextRunDateTime.toString, time.toString)
      tempNextRunDateTime = tempNextRunDateTime + intervalInMillis
    }
    logger.info("getDateTimeInMillisForFirstRun", "_message=\"returning value\" nextRunDateTime={}", nextRunDateTime.toString)
    nextRunDateTime
  }

  private def getDateTimeInMillisForSubsequentRun(handlerConfig: HiveJdbcReaderHandlerConfig, now: Long, lastRunDateTime: Long, properties: util.Map[_ <: String, _]) = {
    val nextRunDateTime = lastRunDateTime + intervalInMillis
    var time: Long = 0
    var tempNextRunDateTime = nextRunDateTime
    while (time == 0 && getTouchFileDate(tempNextRunDateTime) < now) {
      time = getDateTimeInMillis(handlerConfig, now, lastRunDateTime, tempNextRunDateTime, properties)
      logger.info("getDateTimeInMillisForSubsequentRun", "_message=\"subsequent run.\" tempNextRunDateTime={} output={}", tempNextRunDateTime.toString, time.toString)
      tempNextRunDateTime = tempNextRunDateTime + intervalInMillis
    }
    if (time == 0) time
    else nextRunDateTime
  }

  private def getTouchFileDate(nextRunDateTime: Long) = nextRunDateTime + intervalInMillis

  private def getDateTimeInMillis(handlerConfig: HiveJdbcReaderHandlerConfig, now: Long, lastRunDateTime: Long, nextRunDateTime: Long, properties: util.Map[_ <: String, _]) = {
    logger.info("getDateTimeInMillis", "_message=\"will check touchfile.\" now={} nextRunDateTime={} intervalInMillis={}", now.toString, nextRunDateTime.toString, intervalInMillis.toString)
    val tokenizedPath = handlerConfig.getTouchFile
    val yearDtf = DateTimeFormat.forPattern("yyyy")
    val monthDtf = DateTimeFormat.forPattern("MM")
    val dateDtf = DateTimeFormat.forPattern("dd")
    val tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath, "\\$\\{([\\w\\-]+)\\}+")
    val localProperties = new util.HashMap[String, String]
    val tokenSet = tokenToTokenName.keySet
    // if we are loading data for update_date>12/20, the touch file has the date of 12/21. so, we need to add the interval twice.
    val touchFileDate = getTouchFileDate(nextRunDateTime) // this adds a
    // day to the attempted date
    localProperties.put("yyyy", yearDtf.print(touchFileDate))
    localProperties.put("MM", monthDtf.print(touchFileDate))
    localProperties.put("dd", dateDtf.print(touchFileDate))
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
        if (now > touchFileDate && now - touchFileDate > intervalInMillis && now - touchFileDate < 2 * intervalInMillis)
          logger.warn("getDateTimeInMillisForNextRun", "_message=\"file not found\" file_path=\"{}\" exception={}", detokString, e.getMessage)
        else
          logger.info("getDateTimeInMillisForNextRun", "_message=\"file not found\" file_path=\"{}\" exception={}", detokString, e.getMessage)
        0l
      }
    }
  }
}
