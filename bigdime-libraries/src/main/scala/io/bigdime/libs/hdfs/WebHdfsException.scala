package io.bigdime.libs.hdfs

/**
  * Created by neejain on 2/1/17.
  */
case class WebHdfsException(message: String, cause: Throwable, statusCode: Int) extends Exception(message, cause) {
  def this(statusCode: Int, message: String) = {
    this(message, null, statusCode)
  }

  def this(message: String, cause: Throwable) = {
    this(message, cause, 0)
  }
}
