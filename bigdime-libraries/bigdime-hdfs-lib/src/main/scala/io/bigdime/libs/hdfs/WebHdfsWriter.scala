package io.bigdime.libs.hdfs

import java.io.{ByteArrayInputStream, IOException, InputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.http.Header
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import scala.collection.JavaConversions._

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
    * Create the directory specifed by folderPath parameter. Also apply special params such as user.name
    *
    * @param folderPath
    * absolute path representing the directory that needs to be
    * created
    * @throws IOException
    * if there was any problem in executing mkdir command or if the
    * command returned anything by 200 or 201
    */
  @throws[IOException]
  def createDirectory(folderPath: String, params: java.util.Map[String, String] = null) {
    val method = classOf[WebhdfsFacade].getMethod("mkdirs", classOf[String])
    try {
      logger.debug("is webhdfsFacade null=" + (webHdfsFacade == null))
      Option(params) match {
        case Some(_params) => _params.foreach(kv => webHdfsFacade.addParameter(kv._1, kv._2))
      }
      webHdfsFacade.invokeWithRetry[Boolean](method, maxAttempts, folderPath)
    } catch {
      case e: WebHdfsException => throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reasonCode=" + e.statusCode + ", reason=" + e.message)
      case e: Any => throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reason=" + e.getMessage)
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
    createDirectory(folderPath, null)
  }

  /**
    *
    * @param source
    * @param destination
    * @throws IOException
    */
  @throws[IOException]
  def rename(source: String, destination: String): Unit = {
    try {
      webHdfsFacade.rename(source, destination)
    } catch {
      case e: WebHdfsException => throw new WebHDFSSinkException("unable to rename \"" + source + "\" to \"" + destination + "\", reasonCode=" + e.statusCode + ", reason=" + e.message)
      case e: Any => throw new WebHDFSSinkException("unable to rename \"" + source + "\" to \"" + destination + "\", reason=" + e.getMessage)
    }
  }

  /**
    *
    * @param filePath
    * @param payload
    * @param appendMode
    * @throws IOException
    */
  @throws[IOException]
  private def writeToWebHDFS(filePath: String, payload: Array[Byte], appendMode: Boolean) {
    var is: InputStream = new ByteArrayInputStream(payload)
    try {
      val method = if (appendMode) {
        classOf[WebhdfsFacade].getMethod("append", classOf[String], classOf[InputStream])
      } else {
        classOf[WebhdfsFacade].getMethod("createAndWrite", classOf[String], classOf[InputStream])
      }
      webHdfsFacade.invokeWithRetry[Boolean](method, maxAttempts, filePath, is)
    }
    catch {
      case e: WebHdfsException => throw new WebHDFSSinkException("unable to write to hdfs:" + filePath + ", reasonCode=" + e.statusCode + ", reason=" + e.message)
      case e: Any => throw new WebHDFSSinkException("unable to write to hdfs:" + filePath + ", reason=" + e.getMessage)
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
  def write(baseDir: String, payload: Array[Byte], hdfsFileName: String, headers: java.util.Map[String, String] = null, params: java.util.Map[String, String] = null) {
    createDirectory(baseDir, params)
    val filePath = baseDir + FORWARD_SLASH + hdfsFileName
    val fileCreated = try {
      fileExists(filePath)
      true
    }
    catch {
      case e: Exception => false
    }

    Option(headers) match {
      case Some(_map) => _map.foreach(kv => webHdfsFacade.addHeader(kv._1, kv._2))
    }
    Option(params) match {
      case Some(_params) => _params.foreach(kv => webHdfsFacade.addParameter(kv._1, kv._2))
    }
    writeToWebHDFS(filePath, payload, fileCreated)
  }

  def write(baseDir: String, payload: Array[Byte], hdfsFileName: String): Unit = {
    write(baseDir, payload, hdfsFileName, null, null)
  }
}
