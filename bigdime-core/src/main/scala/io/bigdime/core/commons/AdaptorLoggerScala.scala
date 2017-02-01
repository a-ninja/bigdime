package io.bigdime.core.commons

import io.bigdime.alert.{AlertMessage, Logger}
import io.bigdime.core.AdaptorContextImpl

/**
  * Created by neejain on 1/31/17.
  */
case class AdaptorLoggerScala(logger: Logger) {

  import collection.JavaConversions._

  implicit def anyRefToSeqAny(x: AnyRef) = Seq[java.lang.Object](x)

  def debug(shortMessage: String, format: String, o: Any*) {
    logger.debug(getAdaptorName, shortMessage, format, o)
  }

  def info(shortMessage: String, format: String, o: Any*) {
    logger.info(getAdaptorName, shortMessage, format, o)
  }

  def warn(shortMessage: String, format: String, o: Any*) {
    logger.warn(getAdaptorName, shortMessage, format, o)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, message)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String, e: Throwable) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, message, e)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, format: String, o: Any*) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, format, o)
  }

  def alert(alertMessage: AlertMessage) {
    logger.alert(alertMessage)
  }

  protected def getAdaptorName = AdaptorContextImpl.getInstance.getAdaptorName
}
