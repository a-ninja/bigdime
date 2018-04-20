package io.bigdime.libs.hdfs

import java.io.{IOException, InputStream}
import javax.annotation.PostConstruct

import com.typesafe.scalalogging.LazyLogging
import io.bigdime.core.commons.StringHelper
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

/**
  * Created by neejain on 3/17/17.
  */
object WebHdfsReader {
  val FORWARD_SLASH = "/"
  private val WEBHDFS_PREFIX = "/webhdfs/v1"
}

@Component
@Scope("prototype")
class WebHdfsReader extends LazyLogging {

  import WebHdfsReader._

  @Autowired
  private var webHdfsFacade: WebhdfsFacade = _

  @Value("${hdfs_hosts}")
  private var hostNames: String = _

  @Value("${hdfs_port}")
  private var port: Int = 0

  @Value("${hdfs_user}")
  private var hdfsUser: String = _

  @Value("${webhdfs.auth.choice:kerberos}")
  private val authChoice: String = null

  @Value("${webhdfs_max_attempts:5}")
  private val maxAttempts: Short = 0

  private var authOption: HDFS_AUTH_OPTION = _

  @PostConstruct
  @throws[Exception]
  def init() {
    logger.info("PostConstruct: authChoice={} authOption={}", authChoice, authOption)
    authOption = HDFS_AUTH_OPTION.getByName(authChoice)
    logger.info("post PostConstruct: authChoice={} authOption={}", authChoice, authOption)
  }

  def this(_hostNames: String, _port: Int, _hdfsUser: String, _authOption: HDFS_AUTH_OPTION) {
    this()
    hostNames = _hostNames
    port = _port
    hdfsUser = _hdfsUser
    authOption = _authOption
  }

  @throws[IOException]
  @throws[WebHdfsException]
  def getInputStream(hdfsFilePath: String): InputStream = {
    if (StringHelper.isBlank(hdfsFilePath)) throw new IllegalArgumentException("invalid filePath: empty or null")
    val webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath)
    try {
      val method = classOf[WebhdfsFacade].getMethod("open", classOf[String])
      webHdfsFacade.invokeWithRetry[InputStream](method, maxAttempts, webhdfsFilePath)
    } catch {
      case e@(_: NoSuchMethodException | _: SecurityException) => {
        logger.error("method not found", e)
        throw new WebHdfsException("method not found", e)
      }
    }
  }

  /**
    * Uses "LISTSTATUS" operation to list the directory contents and returns
    * the files only.
    * <p>
    * Returns an array of strings naming the files(no directories) in the
    * directory denoted by this abstract pathname. If this abstract pathname
    * does not denote a directory, then this method returns null. Otherwise an
    * array of strings is returned, one for each file in the directory. Names
    * denoting the directory itself and the directory's parent directory are
    * not included in the result. Each string is a complete path.
    * <p>
    * There is no guarantee that the name strings in the resulting array will
    * appear in any specific order; they are not, in particular, guaranteed to
    * appear in alphabetical order.
    *
    * @param hdfsFilePath
    * @param recursive
    * @return
    * @throws WebHdfsException
    * @throws IOException
    * @throws Exception
    */
  @throws[IOException]
  @throws[WebHdfsException]
  def list(hdfsFilePath: String, recursive: Boolean): List[String] = {
    if (StringHelper.isBlank(hdfsFilePath)) throw new IllegalArgumentException("invalid hdfspath: empty or null")
    val webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath)
    fileType(webhdfsFilePath) match {
      case "DIRECTORY" => try {
        val method = classOf[WebhdfsFacade].getMethod("listStatus", classOf[String])
        webHdfsFacade.invokeWithRetry[List[String]](method, maxAttempts, webhdfsFilePath)
      } catch {
        case e@(_: NoSuchMethodException | _: SecurityException) => {
          throw new WebHdfsException("method invocation threw exception:", e)
        }
      }
      case _ => logger.debug("_message=\"processing WebHdfsReader\" webhdfsFilePath={} represents a file", webhdfsFilePath)
        null

    }
  }

  /**
    * Check to see whether the file exists or not.
    *
    * @param hdfsFilePath absolute filepath
    * @return "FILE" if the file is
    * @throws WebHDFSSinkException
    */
  @throws[IOException]
  @throws[WebHdfsException]
  def fileType(hdfsFilePath: String): String = getFileStatus(hdfsFilePath).`type`

  @throws[IOException]
  @throws[WebHdfsException]
  def getFileLength(hdfsFilePath: String): Long = getFileStatus(hdfsFilePath).length

  @throws[IOException]
  @throws[WebHdfsException]
  def getFileStatus(directoryPath: String, fileName: String): FileStatus = {
    var webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(directoryPath)
    webhdfsFilePath = webhdfsFilePath + fileName
    webhdfsFilePath = appendSlash(webhdfsFilePath)
    getFileStatus(webhdfsFilePath)
  }

  @throws[IOException]
  @throws[WebHdfsException]
  def getFileStatusWithoutRetry(hdfsFilePath: String): FileStatus = getFileStatus(hdfsFilePath, 1)

  @throws[IOException]
  @throws[WebHdfsException]
  def getFileStatus(hdfsFilePath: String): FileStatus = getFileStatus(hdfsFilePath, maxAttempts)

  @throws[IOException]
  @throws[WebHdfsException]
  def getFileStatus(hdfsFilePath: String, attempts: Int): FileStatus = {
    if (StringHelper.isBlank(hdfsFilePath)) throw new IllegalArgumentException("invalid filePath: empty or null")
    val webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath)
    try {
      val method = classOf[WebhdfsFacade].getMethod("getFileStatus", classOf[String])
      val fileStatus = webHdfsFacade.invokeWithRetry[FileStatus](method, maxAttempts, webhdfsFilePath)
      fileStatus
    } catch {
      case e@(_: NoSuchMethodException | _: SecurityException) => {
        logger.error("method invocation threw exception:", e)
        throw new WebHdfsException("method invocation threw exception", e)
      }
    }
  }

  def appendSlash(hdfsPath: String): String = {
    if (!StringHelper.isBlank(hdfsPath) && !hdfsPath.endsWith(FORWARD_SLASH)) return hdfsPath + FORWARD_SLASH
    hdfsPath
  }

  def prependWebhdfsPrefixAndAppendSlash(hdfsPathWithoutPrefix: String): String = {
    var hdfsPath = hdfsPathWithoutPrefix
    if (!StringHelper.isBlank(hdfsPath) && !hdfsPath.startsWith(WEBHDFS_PREFIX)) hdfsPath = WEBHDFS_PREFIX + hdfsPath
    appendSlash(hdfsPath)
  }

  def getHostNames: String = hostNames

  def getPort: Int = port

  def getHdfsUser: String = hdfsUser

  def getAuthChoice: String = authChoice

  def getMaxAttempts: Short = maxAttempts

  def getAuthOption: HDFS_AUTH_OPTION = authOption

  override def toString: String = "WebHdfsReader [hostNames=" + hostNames + ", port=" + port + ", hdfsUser=" + hdfsUser + ", authChoice=" + authChoice + ", maxAttempts=" + maxAttempts + ", authOption=" + authOption + "]"
}
