package io.bigdime.handler.hive

import java.util
import java.util.concurrent.TimeUnit

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.commons.AdaptorLogger

/**
  * Created by neejain on 2/6/17.
  */
object LatencyBasedNextRunChecker {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[LatencyBasedNextRunChecker]))
  private val intervalInMillis = TimeUnit.DAYS.toMillis(1)
}

case class LatencyBasedNextRunChecker() extends AbstractNextRunChecker {

  import LatencyBasedNextRunChecker._

  override def getDateTimeInMillisForNextRun(lastRunDateTime: Long, handlerConfig: HiveJdbcReaderHandlerConfig, properties: util.Map[_ <: String, _]): Long = {
    val now = getAdjustedCurrentTime(handlerConfig.getLatency)
    if (lastRunDateTime == 0) {
      // this is the first time
      getDateTimeInMillisForFirstRun(handlerConfig, now)
    } else getDateTimeInMillisForSubsequentRun(handlerConfig, now, lastRunDateTime)
  }

  protected def getDateTimeInMillisForSubsequentRun(handlerConfig: HiveJdbcReaderHandlerConfig, now: Long, lastRunDateTime: Long): Long = {
    var nextRunDateTime = 0l
    if (now - lastRunDateTime > handlerConfig.getMinGoBack) {
      nextRunDateTime = lastRunDateTime + intervalInMillis
      logger.info("getDateTimeInMillisForNextRun", "_message=\"time to set hiveConfDateTime.\" now={} lastRunDateTime={} hiveConfDateTime={} intervalInMillis={}", now: java.lang.Long, lastRunDateTime: java.lang.Long, nextRunDateTime: java.lang.Long, intervalInMillis: java.lang.Long)
    }
    else logger.info("nothing to do", "now={} hiveConfDateTime={} intervalInMillis={} time_until_next_run={}", now: java.lang.Long, lastRunDateTime: java.lang.Long, intervalInMillis: java.lang.Long, (handlerConfig.getMinGoBack - (now - lastRunDateTime)): java.lang.Long)
    nextRunDateTime
  }
}
