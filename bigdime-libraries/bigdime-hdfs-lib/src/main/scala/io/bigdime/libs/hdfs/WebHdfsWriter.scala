package io.bigdime.libs.hdfs

import java.io.{ByteArrayInputStream, IOException, InputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpResponse
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

/**
  * Created by neejain on 3/17/17.
  */
object WebHdfsWriter {
  val FORWARD_SLASH = "/"
}

@Component
@Scope("prototype")
class WebHdfsWriter extends LazyLogging {

  import WebHdfsWriter.FORWARD_SLASH

  private val sleepTime = 3000
  @Value("${hdfs_hosts}")
  private var hostNames: String = _

  @Value("${hdfs_port}")
  private var port: Int = 0

  @Value("${hdfs_user}")
  private var hdfsUser: String = _

  @Value("${webhdfs.auth.choice:kerberos}")
  private var authChoice: String = null

  @Value("${webhdfs_max_attempts:5}")
  private val maxAttempts = 0

  private[hdfs] val authOption: HDFS_AUTH_OPTION = null

  /**
    * Check to see whether the file exists or not.
    *
    * @param filePath
    * absolute filepath
    * @return true if the file exists, false otherwise
    * @throws WebHDFSSinkException
    */
  @throws[WebHDFSSinkException]
  private def fileExists(webHdfs: WebHdfs, filePath: String) = {
    try {
      val facade = new WebhdfsFacade(hostNames, port, authOption)
      val method = classOf[WebhdfsFacade].getMethod("getFileStatus", classOf[String])
      facade.invokeWithRetry(method, 1, filePath)
      true
    } catch {
      case e: WebHdfsException => logger.warn("WebHdfs File Status Failed: The Sink or Data Writer could not check the status of the file:\" status={} reason={} error={}", e.statusCode.toString, e.message, e)
        throw new WebHDFSSinkException("could not get the file status", e)
      case e: Exception => {
        logger.warn("WebHdfs File Status Failed: The Sink or Data Writer could not check the status of the file:\" error={}", e)
        throw new WebHDFSSinkException("could not get the file status", e)
      }
    }
  }

  /**
    * Create the directory specifed by folderPath parameter.
    *
    * @param webHdfs
    * { @link WebHdfs} object that actually invokes the call to write
    * to hdfs
    * @param folderPath
    * absolute path representing the directory that needs to be
    * created
    * @throws IOException
    * if there was any problem in executing mkdir command or if the
    * command returned anything by 200 or 201
    */
  @throws[IOException]
  def createDirectory(webHdfs: WebHdfs, folderPath: String) {
    val facade = WebhdfsFacade(hostNames, port, authOption)
    val method = classOf[WebhdfsFacade].getMethod("mkdir", classOf[String])
    try {
      facade.invokeWithRetry[Boolean](method, maxAttempts, folderPath)
    } catch {
      case e: WebHdfsException => throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reasonCode=" + e.statusCode + ", reason=" + e.message)
      case e: Any => throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reason=" + e.getMessage)
    }
  }

  @throws[IOException]
  private def writeToWebHDFS(webHdfs: WebHdfs, filePath: String, payload: Array[Byte], appendMode: Boolean) {
    var response: HttpResponse = null
    var isSuccess = false
    var retry = 0
    var exceptionReason: String = null
    try
        do {
          var is: InputStream = null
          retry += 1
          try {
            is = new ByteArrayInputStream(payload)
            if (appendMode) response = webHdfs.append(filePath, is)
            else response = webHdfs.createAndWrite(filePath, is)
            if (response.getStatusLine.getStatusCode == 201 || response.getStatusLine.getStatusCode == 200) isSuccess = true
            else {
              exceptionReason = response.getStatusLine.getStatusCode + ":" + response.getStatusLine.getReasonPhrase
              logger.warn("_message=\"WebHdfs Data Write Failed\" status_code={} reason={} retry={} filePath={} host={}", response.getStatusLine.getStatusCode.toString, response.getStatusLine.getReasonPhrase, retry.toString, filePath, webHdfs.getHost)
              Thread.sleep(sleepTime * (retry + 1))
            }
          } catch {
            case e: Exception => {
              exceptionReason = e.getMessage
              logger.warn("_message=\"WebHdfs Data Write Failed: The Sink or Data Writer could not write the data to HDFS.:\" retry={} filePath={} host ={} error={}", retry.toString, filePath, webHdfs.getHost, e)
            }
          } finally if (is != null) is.close()
        } while (!isSuccess && retry <= 3)
    finally webHdfs.releaseConnection()
    if (!isSuccess) {
      logger.error("_message=\"WebHdfs Data Write Failed After 3 retries : The Sink or Data Writer could not write the data to HDFS.:\"")
      throw new WebHDFSSinkException(exceptionReason)
    }
  }

  /**
    * Facade method to write data to a file in hdfs. If the file or directory
    * does not exist, this method creates them.
    *
    * @param baseDir
    * directory hosting the file where payload needs to be written
    * to
    * @param payload
    * data to be written to file
    * @param hdfsFileName
    * name of the file on hdfs
    * @throws IOException
    * if there was any problem in writing the data
    */
  @throws[IOException]
  def write(webHdfs: WebHdfs, baseDir: String, payload: Array[Byte], hdfsFileName: String) {
    createDirectory(webHdfs, baseDir)
    val filePath = baseDir + FORWARD_SLASH + hdfsFileName
    val fileCreated = fileExists(webHdfs, filePath)
    writeToWebHDFS(webHdfs, filePath, payload, fileCreated)
  }
}
