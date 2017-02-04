package io.bigdime.alert.impl

/**
  * Created by neejain on 2/4/17.
  */
abstract class AbstractLogger {

  private var debugEnabled = false
  private var infoEnabled = false
  private var warnEnabled = false
  protected def isDebugEnabled: Boolean = debugEnabled

  protected def isInfoEnabled: Boolean = infoEnabled

  protected def isWarnEnabled: Boolean = warnEnabled

  protected def setDebugEnabled() {
    debugEnabled = true
    setInfoEnabled
  }

  protected def setInfoEnabled() {
    infoEnabled = true
    setWarnEnabled()
  }

  protected def setWarnEnabled() {
    warnEnabled = true
  }
}
