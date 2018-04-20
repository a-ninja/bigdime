package io.bigdime.libs.hdfs

import java.io.IOException

/**
  * Created by neejain on 3/16/17.
  */
object WebHDFSSinkException {
  val serialVersionUID: Long = 1L
}

case class WebHDFSSinkException(_message: String, _cause: Throwable) extends IOException(_message, _cause) {
  def this(message: String) {
    this(message, null)
  }
}
