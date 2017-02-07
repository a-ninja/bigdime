package io.bigdime.handler.hive

import java.util.concurrent.TimeUnit

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.commons.AdaptorLogger
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by neejain on 2/6/17.
  */
object AbstractNextRunChecker {
  private val logger: AdaptorLogger = new AdaptorLogger(LoggerFactory.getLogger(classOf[AbstractNextRunChecker]))
}

abstract class AbstractNextRunChecker extends HiveNextRunChecker {

  import AbstractNextRunChecker.logger

  private val dateTimeZone: DateTimeZone = DateTimeZone.forID("America/Los_Angeles")

  protected def getAdjustedCurrentTime: Long = {
    getAdjustedCurrentTime(0)
  }

  protected def getAdjustedCurrentTime(latency: Long): Long = {
    val systemTime: Long = System.currentTimeMillis
    val nowPacific = DateTime.now(dateTimeZone).getMillis - latency
    logger.info("getAdjustedCurrentTime", "systemTime={} latency={} now={}", systemTime, latency, nowPacific)
    nowPacific
  }

  protected def getDateTimeInMillisForFirstRun(handlerConfig: HiveJdbcReaderHandlerConfig, now: Long): Long = {
    val nextRunDateTime: Long = now - handlerConfig.getGoBackDays * TimeUnit.DAYS.toMillis(1)
    logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run, set hiveConfDateTime done. no need to check for the touchFile, it may not even be present.\" hiveConfDateTime={}", nextRunDateTime)
    nextRunDateTime
  }
}
