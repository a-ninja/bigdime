package io.bigdime.core.commons

import io.bigdime.alert.{AlertMessage, Logger}
import io.bigdime.core.AdaptorContextImpl

/**
  * Created by neejain on 1/31/17.
  */
case class AdaptorLoggerScala(logger: Logger) {

  implicit def anyRefToSeqAny(x: AnyRef) = Seq[java.lang.Object](x)

  def debug(shortMessage: String, format: String, o: AnyRef*) {
    logger.debug(getAdaptorName, shortMessage, format, o: _*)
  }

  def info(shortMessage: String, format: String, o: AnyRef*) {
    logger.info(getAdaptorName, shortMessage, format, o: _*)
  }

  def warn(shortMessage: String, format: String, o: AnyRef*) {
    logger.warn(getAdaptorName, shortMessage, format, o: _*)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, message)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String, e: Throwable) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, message, e)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, format: String, o: AnyRef*) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, null.asInstanceOf[Throwable], format, o: _*)
  }

  def alert(alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, t: Throwable, format: String, o: AnyRef*) {
    logger.alert(getAdaptorName, alertType, alertCause, alertSeverity, t, format, o: _*)
  }

  def alert(alertMessage: AlertMessage) {
    logger.alert(alertMessage)
  }

  protected def getAdaptorName = AdaptorContextImpl.getInstance.getAdaptorName
}
