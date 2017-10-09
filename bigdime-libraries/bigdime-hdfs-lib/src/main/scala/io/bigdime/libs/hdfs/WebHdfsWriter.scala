package io.bigdime.libs.hdfs

import java.io.{ByteArrayInputStream, IOException, InputStream}

import com.typesafe.scalalogging.LazyLogging
import org.springframework.beans.factory.annotation.{Autowired, Value}
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

  @Autowired
  private var webHdfsFacade: WebhdfsFacade = _

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
    * TODO: how is it returning false?
    *
    * @param filePath
    * absolute filepath
    * @return true if the file exists, false otherwise
    * @throws WebHDFSSinkException
    */
  @throws[WebHDFSSinkException]
  private def fileExists(filePath: String) = {
    try {
      //      val facade = new WebhdfsFacade(hostNames, port, authOption)
      val method = classOf[WebhdfsFacade].getMethod("getFileStatus", classOf[String])
      webHdfsFacade.invokeWithRetry(method, 1, filePath)
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
    * @param folderPath
    * absolute path representing the directory that needs to be
    * created
    * @throws IOException
    * if there was any problem in executing mkdir command or if the
    * command returned anything by 200 or 201
    */
  @throws[IOException]
  def createDirectory(folderPath: String) {
    //    val facade = WebhdfsFacade(hostNames, port, authOption)
    val method = classOf[WebhdfsFacade].getMethod("mkdirs", classOf[String])
    try {
      webHdfsFacade.invokeWithRetry[Boolean](method, maxAttempts, folderPath)
    } catch {
      case e: WebHdfsException => throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reasonCode=" + e.statusCode + ", reason=" + e.message)
      case e: Any => throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reason=" + e.getMessage)
    }
  }

  @throws[IOException]
  private def writeToWebHDFS(filePath: String, payload: Array[Byte], appendMode: Boolean) {
    var is: InputStream = new ByteArrayInputStream(payload)
    try {
      //      val facade = WebhdfsFacade(hostNames, port, authOption)
      val method = if (appendMode) {
        classOf[WebhdfsFacade].getMethod("append", classOf[String], classOf[InputStream])
      } else {
        classOf[WebhdfsFacade].getMethod("createAndWrite", classOf[String], classOf[InputStream])
      }
      webHdfsFacade.invokeWithRetry[Boolean](method, maxAttempts, filePath, is)
    }
    catch {
      case e: WebHdfsException => throw new WebHDFSSinkException("unable to write to hdfs:" + filePath + ", reasonCode=" + e.statusCode + ", reason=" + e.message)
      case e: Any => throw new WebHDFSSinkException("unable to create directory:" + filePath + ", reason=" + e.getMessage)
    }
    finally if (is != null) is.close()
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
  def write(baseDir: String, payload: Array[Byte], hdfsFileName: String) {
    createDirectory(baseDir)
    val filePath = baseDir + FORWARD_SLASH + hdfsFileName
    val fileCreated = try {
      fileExists(filePath)
      true
    }
    catch {
      case e: Exception => false
    }
    writeToWebHDFS(filePath, payload, fileCreated)
  }
}
