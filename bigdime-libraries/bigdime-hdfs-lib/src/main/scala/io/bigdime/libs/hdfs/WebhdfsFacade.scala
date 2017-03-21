package io.bigdime.libs.hdfs

import java.io.{File, IOException, InputStream}
import java.lang.reflect.Method
import java.net.{URI, URISyntaxException}

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import io.bigdime.util.TryAndGiveUp
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods._
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{AbstractHttpEntity, BasicHttpEntity, FileEntity}
import org.apache.http.{Header, HttpResponse}

import scala.util.{Failure, Success, Try}

/**
  * Wrapper class for webhdfs REST operations.
  * Created by neejain on 3/21/17.
  */
//not a threadsafe implementation
case class WebhdfsFacade(hosts: String, port: Int, authOption: HDFS_AUTH_OPTION = HDFS_AUTH_OPTION.KERBEROS) extends LazyLogging {
  private var jsonParameters = (new ObjectMapper()).createObjectNode
  private var httpRequest: HttpRequestBase = null

  private var headers: List[Header] = null
  private var activeHost: String = rotateHost()
  private val scheme = new URI(activeHost).getScheme
  private val webhdfsHttpClient = WebHdfsHttpClientFactory(authOption)

  protected def rotateHost(): String = {
    logger.info("_message=\"rotating host\" hosts={} current_active_host={}", hosts, activeHost)
    activeHost = ActiveNameNodeResolver.rotateHost(hosts, activeHost)
    logger.info("_message=\"rotated host\" hosts={} new_active_host={}", hosts, activeHost)
    activeHost
  }


  /**
    * Wrapper to call initConnection of WebHdfsHttpClient.
    * The client only needs to be initialized in the beginning or in case of a 403.
    */
  private def initConnection() = webhdfsHttpClient.init()

  protected def handleForbidden() = {
    rotateHost()
    initConnection()
  }

  def addHeaders(headers: List[Header]) = {
    this.headers = headers
  }

  def addParameter(key: String, value: String): WebhdfsFacade = {
    jsonParameters.put(key, value)
    this
  }


  protected def buildURI(op: String, HdfsPath: String): URI = {
    try {
      val uriBuilder = new URIBuilder
      val uri = new URI(activeHost)
      uriBuilder.setScheme(uri.getScheme).setHost(uri.getHost).setPort(this.port).setPath(HdfsPath).addParameter("op", op)
      val keys = jsonParameters.fieldNames()
      while (keys.hasNext) {
        val key = keys.next
        val value = jsonParameters.get(key)
        val valueStr = value.asText()
        if (valueStr != null) uriBuilder.addParameter(key, valueStr)
      }
      uriBuilder.build
    }
    catch {
      case e: URISyntaxException => {
        null
      }
    }
  }

  /**
    * MKDIR or RENAME
    *
    * @param uri
    * @param proc
    * @tparam R
    * @tparam T
    * //    * @throws
    * //    * @throws
    * @return
    */
  @throws[ClientProtocolException]
  @throws[IOException]
  private def put[R <: WebhdfsResponseProcessor[T], T](uri: URI, proc: R) = {
    webhdfsHttpClient.execute(scheme, new HttpPut(uri), proc)
  }

  // CREATE
  @throws[ClientProtocolException]
  @throws[IOException]
  private def put[R <: WebhdfsResponseProcessor[T], T](uri: URI, filePath: String, proc: R) = {
    val httpRequest = new HttpPut(uri)
    //    val httpReq = getRedirectedRequest[HttpPut](httpRequest, new FileEntity(new File(filePath)))
    val httpReq = getRedirectedRequest(httpRequest)(classOf[HttpPut], new FileEntity(new File(filePath)))
    webhdfsHttpClient.execute(scheme, httpReq, proc)
  }

  // CREATE
  @throws[ClientProtocolException]
  @throws[IOException]
  private def put[R <: WebhdfsResponseProcessor[T], T](uri: URI, in: InputStream, proc: R) = {
    val httpRequest = new HttpPut(uri)
    val entity = new BasicHttpEntity
    entity.setContent(in)
    //    val httpReq = getRedirectedRequest[HttpPut](httpRequest, entity)
    val httpReq = getRedirectedRequest(httpRequest)(classOf[HttpPut], entity)
    webhdfsHttpClient.execute(scheme, httpReq, proc)
  }

  def getRedirectedRequest(request: HttpRequestBase) = {
    def temporaryRedirectURI(request: HttpRequestBase): String = {
      webhdfsHttpClient.execute(scheme, request, RedirectLocationHandler()).get
    }

    (r: Class[_ <: HttpEntityEnclosingRequestBase], entity: AbstractHttpEntity) => {
      val inst = r.getConstructor(classOf[String]).newInstance(temporaryRedirectURI(request))
      inst.setEntity(entity)
      inst
    }
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  private def delete[R <: WebhdfsResponseProcessor[T], T](uri: URI, proc: R) = {
    webhdfsHttpClient.execute(scheme, new HttpDelete(uri), proc)
  }

  // LISTSTATUS, OPEN, GETFILESTATUS, GETCHECKSUM,
  @throws[ClientProtocolException]
  @throws[IOException]
  protected def getAndConsume[R <: WebhdfsResponseProcessor[T], T](uri: URI, proc: R): Try[T] = {
    webhdfsHttpClient.execute(scheme, new HttpGet(uri), proc)
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  protected def get(uri: URI): HttpResponse = {
    val req = new HttpGet(uri)
    try {
      webhdfsHttpClient.execute(scheme, new HttpGet(uri))
    } catch {
      case ex: Throwable => req.releaseConnection()
        throw ex
    }
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def createAndWrite(webhdfsPath: String, inputStream: InputStream): Try[Boolean] = {
    put[BooleanResponseHandler, Boolean](buildURI("CREATE", webhdfsPath), inputStream, BooleanResponseHandler())
    //    process[BooleanResponseHandler, Boolean](buildURI)("CREATE", webhdfsPath)(put)(inputStream, BooleanResponseHandler())
  }

  /**
    * open method handles the HttpResponse itself,
    * rather than letting the CloseableHttpClient handle it, since it needs to keep the connection open to let the caller read from the stream.
    *
    * @param webhdfsPath
    * @throws ClientProtocolException
    * @throws IOException
    * @return
    */
  @throws[ClientProtocolException]
  @throws[IOException]
  def open(webhdfsPath: String): Try[InputStream] = {
    InputStreamResponseHandler().handleResponse(get(buildURI("OPEN", webhdfsPath)))
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def mkdirs(hdfsPath: String): Try[Boolean] = {
    put[BooleanResponseHandler, Boolean](buildURI("MKDIRS", hdfsPath), BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def rename(webHdfsPath: String, toHdfsPath: String): Try[Boolean] = {
    addParameter(WebHDFSConstants.DESTINATION, toHdfsPath)
    put[BooleanResponseHandler, Boolean](buildURI("RENAME", webHdfsPath), BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def delete(webHdfsPath: String): Try[Boolean] = {
    addParameter(WebHDFSConstants.DESTINATION, webHdfsPath)
    delete[BooleanResponseHandler, Boolean](buildURI("DELETE", webHdfsPath), BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def getFileStatus(webhdfsPath: String): Try[FileStatus] = {
    getAndConsume[FileStatusResponseHandler, FileStatus](buildURI("GETFILESTATUS", webhdfsPath), FileStatusResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def listStatus(webhdfsPath: String): Try[List[String]] = {
    getAndConsume[ListStatusResponseHandler, List[String]](buildURI("LISTSTATUS", webhdfsPath), ListStatusResponseHandler(webhdfsPath))
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def getFileChecksum(webhdfsPath: String): Try[HdfsFileChecksum] = {
    getAndConsume[ChecksumResponseHandler, HdfsFileChecksum](buildURI("GETFILECHECKSUM", webhdfsPath), ChecksumResponseHandler())
  }

  @annotation.varargs
  @throws[WebHdfsException]
  def invokeWithRetry[T](method: Method, maxAttempts: Int, args: AnyRef*): T = {
    var isSuccess = false
    var statusCode = 0
    var exceptionReason: String = null
    var attempts = 0
    try {
      do {
        attempts += 1
        logger.debug("_message=\"invoking {}\" attempt={} args={} http_request_null={}", method.getName, attempts: java.lang.Integer, args)
        try {
          val response = method.invoke(this, args: _*).asInstanceOf[Try[T]]
          response match {
            case Success(t) => return t
            case Failure(e: WebHdfsException) =>
              statusCode = e.statusCode
              exceptionReason = e.message
              e.statusCode match {
                case 401 =>
                  logger.info("_message=\"executed method: {}\" unauthorized:\"{}\"", method.getName, args)
                case 403 =>
                  logger.info("_message=\"executed method: {}\" forbidden:\"{}\"", method.getName, args)
                  handleForbidden()
                case 404 =>
                  logger.info("_message=\"executed method: {}\" file not found:\"{}\"", method.getName, args)
                case 500 =>
                  logResponse(e, method.getName, attempts, args: _*)
                  attempts -= 1
              }
          }
        }
        catch {
          case e: Throwable => {
            exceptionReason = e.getMessage
            logger.warn("_message=\"{} failed with exception:\"", method.getName, e)
            sleepUninterrupted(attempts)
          }
        }
      } while (!isSuccess && attempts < maxAttempts)
    }
    catch {
      case e1: SecurityException => {
        logger.error("_message=\"{} failed:\"", method.getName, e1)
      }
    }
    logger.warn("_message=\"{} failed After {} retries :\", statusCode={} exceptionReason={} args={}", method.getName, maxAttempts: java.lang.Integer, statusCode: java.lang.Integer, exceptionReason, args)
    throw new WebHdfsException(statusCode, exceptionReason)
  }

  val SLEEP_TIME = 3000

  private def logResponse(e: WebHdfsException, message: String, attempts: Int, args: AnyRef*) {
    val statusCode = e.statusCode
    logger.warn("_message=\"{} failed\" responseCode={} responseMessage={} attempts={} args={}", message, statusCode.toString, e.message, attempts.toString, args)
    sleepUninterrupted(attempts)
  }

  private def sleepUninterrupted(attempts: Int) {
    TryAndGiveUp()(() => {
      Thread.sleep(SLEEP_TIME * (attempts + 1))
    })
  }
}

