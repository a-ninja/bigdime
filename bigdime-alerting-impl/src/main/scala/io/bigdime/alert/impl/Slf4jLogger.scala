package io.bigdime.alert.impl

import java.util.concurrent.ConcurrentHashMap

import io.bigdime.alert.{AlertMessage, Logger}

/**
  * Created by neejain on 2/4/17.
  */
object Slf4jLogger {
  private val loggerMap = new ConcurrentHashMap[String, Slf4jLogger]

  def getLogger(loggerName: String): Logger = {
    var logger = loggerMap.get(loggerName)
    if (logger == null) {
      logger = new Slf4jLogger
      loggerMap.put(loggerName, logger)
      logger.slf4jLogger = org.slf4j.LoggerFactory.getLogger(loggerName)
    }
    logger
  }
}

class Slf4jLogger private() extends AbstractLogger with Logger {
  private var slf4jLogger: org.slf4j.Logger = null

  def debug(source: String, shortMessage: String, message: String) {
    slf4jLogger.debug("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage, message)
  }

  def debug(source: String, shortMessage: String, format: String, o: Object*) {
    if (slf4jLogger.isDebugEnabled) {
      val sb: StringBuilder = new StringBuilder
      val argArray: Array[AnyRef] = buildArgArray(sb, source, shortMessage, format, o: _*)
      slf4jLogger.debug(sb.toString, argArray: _*)
    }
  }

  def info(source: String, shortMessage: String, message: String) {
    slf4jLogger.info("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage, message)
  }

  def info(source: String, shortMessage: String, format: String, o: Object*) {
    if (slf4jLogger.isInfoEnabled) {
      val sb: StringBuilder = new StringBuilder
      val argArray: Array[AnyRef] = buildArgArray(sb, source, shortMessage, format, o: _*)
      slf4jLogger.info(sb.toString, argArray: _*)
    }
  }

  def warn(source: String, shortMessage: String, message: String) {
    slf4jLogger.warn("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage, message)
  }

  def warn(source: String, shortMessage: String, format: String, o: Object*) {
    if (slf4jLogger.isWarnEnabled) {
      val sb: StringBuilder = new StringBuilder
      val argArray: Array[AnyRef] = buildArgArray(sb, source, shortMessage, format, o: _*)
      slf4jLogger.warn(sb.toString, argArray: _*)
    }
  }

  def warn(source: String, shortMessage: String, message: String, t: Throwable) {
    slf4jLogger.warn("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage, message, t)
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String) {
    slf4jLogger.error("adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\" detail_message=\"{}\"", source, alertSeverity, "TODO:set context", alertType.getMessageCode, alertType.getDescription, alertCause.getDescription, message)
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String, t: Throwable) {
    slf4jLogger.error("adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\" detail_message=\"{}\"", source, alertSeverity, "TODO:set context", alertType.getMessageCode, alertType.getDescription, alertCause.getDescription, message, t)
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, t: Throwable, format: String, o: Object*) {
    val sb: StringBuilder = new StringBuilder
    sb.append("adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\"").append(" ").append(format)
    val argArray: Array[AnyRef] = new Array[AnyRef](6 + o.length + 1)
    argArray(0) = source
    argArray(1) = alertSeverity
    argArray(2) = "todo: set context"
    argArray(3) = alertType.getMessageCode
    argArray(4) = alertType.getDescription
    argArray(5) = alertCause.getDescription
    var i: Int = 5
    for (o1 <- o) {
      i += 1
      argArray(i) = o1
    }
    i += 1
    argArray(i) = t
    slf4jLogger.error(sb.toString, argArray: _*)
  }

  def alert(message: AlertMessage) {
    alert(message.getAdaptorName, message.getType, message.getCause, message.getSeverity, message.getMessage)
  }

  private def buildArgArray(sb: StringBuilder, source: String, shortMessage: String, format: String, o: Object*): Array[AnyRef] = {
    sb.append("adaptor_name=\"{}\" message_context=\"{}\"").append(" ").append(format)
    val argArray = new Array[AnyRef](2 + o.length)
    argArray(0) = source
    argArray(1) = shortMessage
    var i: Int = 1

    for (o1 <- o) {
      i += 1
      argArray(i) = o1
    }
    return argArray
  }
}
