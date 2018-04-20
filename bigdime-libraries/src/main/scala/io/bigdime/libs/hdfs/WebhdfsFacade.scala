package io.bigdime.libs.hdfs

import java.io.{File, IOException, InputStream}
import java.lang.reflect.Method
import java.net.{URI, URISyntaxException}

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import io.bigdime.util.{Retry, SleepUninterrupted}
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods._
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.{AbstractHttpEntity, BasicHttpEntity, FileEntity}
import org.apache.http.message.BasicHeader
import org.apache.http.{Header, HttpResponse}
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Wrapper class for webhdfs REST operations.
  * Created by neejain on 3/21/17.
  */
//not a threadsafe implementation
@Component
@Scope("prototype")
case class WebhdfsFacade(@Value("${hdfs_hosts}") hosts: String, @Value("${hdfs_port}") port: Int, @Value("${webhdfs.auth.choice:kerberos}") authChoice: String) extends LazyLogging {
  private val authOption = HDFS_AUTH_OPTION.getByName(authChoice)
  private var jsonParameters = (new ObjectMapper).createObjectNode
  private var httpRequest: HttpRequestBase = _

  private var headers: ListBuffer[Header] = new ListBuffer[Header]()
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

  def addHeader(key: String, value: String): Unit = {
    headers.append(new BasicHeader(key, value))
  }


  def addParameter(key: String, value: String): WebhdfsFacade = {
    jsonParameters.put(key, value)
    this
  }

  /**
    * Return a function that can accept a HttpRequestBase and execute it to return HttpRespo
    *
    * @param block
    * @param req
    * @tparam T
    * @return
    */
  def executeAndHandle[T](block: (HttpRequestBase) => T)(req: HttpRequestBase) = {
    Retry(1, List(classOf[Throwable]), 0, (t) => {
      req.releaseConnection()
      throw t
    })(() => {
      block(req)
    }).get
  }

  /**
    * Call the underlying method that executes the request and delegates the response to proc. The response is completely consumed.
    *
    * @param request
    * @param proc
    * //    * @tparam R
    * @tparam T
    * @return
    */
  def execute[T](request: HttpRequestBase, proc: WebhdfsResponseProcessor[T]): Try[T] = {
    webhdfsHttpClient.execute(scheme, request, proc)
  }

  /**
    * Call the underlying method that executes the request and returns the response. Caller must consume the reponse and release the connection.
    *
    * @param request
    * @tparam R
    * @tparam T
    * @return
    */
  def execute[R <: WebhdfsResponseProcessor[T], T](request: HttpRequestBase): HttpResponse = {
    webhdfsHttpClient.execute(scheme, request)
  }

  /**
    * internal function to build URI from various parameters
    *
    * @param op
    * @param HdfsPath
    * @return
    */
  protected def buildURI(op: String)(implicit HdfsPath: String): URI = {
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
      val newUri = uriBuilder.build()
      logger.debug("after build, uri={}", newUri.toString)
      newUri
    }
    catch {
      case e: URISyntaxException => {
        logger.error("mkdirs failed ", e)
        null
      }
    }
  }

  /**
    * MKDIR or RENAME
    *
    * @param uri
    * @param proc
    * //    * @tparam R
    * @tparam T
    * //    * @throws
    * //    * @throws
    * @return
    */
  @throws[ClientProtocolException]
  @throws[IOException]
  private def put[T](uri: URI, proc: WebhdfsResponseProcessor[T]) = {
    execute(new HttpPut(uri), proc)
  }

  // CREATE
  @throws[ClientProtocolException]
  @throws[IOException]
  private def put[T](uri: URI, filePath: String, proc: WebhdfsResponseProcessor[T]) = update(classOf[HttpPut], uri, filePath, proc)

  // CREATE
  @throws[ClientProtocolException]
  @throws[IOException]
  private def put[T](uri: URI, in: InputStream, proc: WebhdfsResponseProcessor[T]) = update(classOf[HttpPut], uri, in, proc)

  // APPEND
  @throws[ClientProtocolException]
  @throws[IOException]
  private def post[T](uri: URI, filePath: String, proc: WebhdfsResponseProcessor[T]) = update(classOf[HttpPost], uri, filePath, proc)

  // APPEND
  @throws[ClientProtocolException]
  @throws[IOException]
  private def post[T](uri: URI, in: InputStream, proc: WebhdfsResponseProcessor[T]) = update(classOf[HttpPost], uri, in, proc)

  // APPEND
  @throws[ClientProtocolException]
  @throws[IOException]
  private def update[T](clazz: Class[_ <: HttpEntityEnclosingRequestBase], uri: URI, filePath: String, proc: WebhdfsResponseProcessor[T]) = {
    val httpRequest = clazz.getConstructor(classOf[URI]).newInstance(uri)
    val httpReq = getRedirectedRequest(httpRequest)(clazz, new FileEntity(new File(filePath)))
    headers.foreach(h => httpReq.addHeader(h))
    execute(httpReq, proc)
  }

  // APPEND
  @throws[ClientProtocolException]
  @throws[IOException]
  private def update[T](clazz: Class[_ <: HttpEntityEnclosingRequestBase], uri: URI, in: InputStream, proc: WebhdfsResponseProcessor[T]) = {
    val httpRequest = clazz.getConstructor(classOf[URI]).newInstance(uri)
    val entity = new BasicHttpEntity
    entity.setContent(in)
    val httpReq = getRedirectedRequest(httpRequest)(clazz, entity)
    headers.foreach(h => httpReq.addHeader(h))
    execute(httpReq, proc)
  }

  def getRedirectedRequest(request: HttpRequestBase) = {
    def temporaryRedirectURI(request: HttpRequestBase): String = {
      execute[String](request, RedirectLocationHandler()).get
    }

    (httpRequestBaseCls: Class[_ <: HttpEntityEnclosingRequestBase], entity: AbstractHttpEntity) => {
      val httpRequest = httpRequestBaseCls.getConstructor(classOf[String]).newInstance(temporaryRedirectURI(request))
      httpRequest.setEntity(entity)
      httpRequest
    }
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  private def delete[T](uri: URI, proc: WebhdfsResponseProcessor[T]) = {
    execute(new HttpDelete(uri), proc)
  }

  // LISTSTATUS, OPEN, GETFILESTATUS, GETCHECKSUM,
  @throws[ClientProtocolException]
  @throws[IOException]
  protected def getAndConsume[T](uri: URI, proc: WebhdfsResponseProcessor[T]): Try[T] = {
    execute(new HttpGet(uri), proc)
  }


  /**
    * Get and do NOT release the connection
    *
    * @param uri
    * @throws ClientProtocolException
    * @throws IOException
    * @return
    */
  @throws[ClientProtocolException]
  @throws[IOException]
  protected def get(uri: URI): HttpResponse = {
    executeAndHandle((r) => execute(r))(new HttpGet(uri))
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def createAndWrite(implicit webhdfsPath: String, in: InputStream): Try[Boolean] = {
    put(buildURI("CREATE"), in, BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def createAndWrite(webhdfsPath: String, filePath: String): Try[Boolean] = {
    put(buildURI("CREATE")(webhdfsPath), filePath, BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def append(implicit webhdfsPath: String, in: InputStream): Try[Boolean] = {
    post(buildURI("APPEND"), in, BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def append(webhdfsPath: String, filePath: String): Try[Boolean] = {
    post(buildURI("APPEND")(webhdfsPath), filePath, BooleanResponseHandler())
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
  def open(implicit webhdfsPath: String): Try[InputStream] = {
    InputStreamResponseHandler().handleResponse(get(buildURI("OPEN")))
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def mkdirs(implicit hdfsPath: String): Try[Boolean] = {
    put(buildURI("MKDIRS"), BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def rename(webHdfsPath: String, toHdfsPath: String): Try[Boolean] = {
    addParameter(WebHDFSConstants.DESTINATION, toHdfsPath)
    put(buildURI("RENAME")(webHdfsPath), BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def delete(implicit webHdfsPath: String): Try[Boolean] = {
    addParameter(WebHDFSConstants.DESTINATION, webHdfsPath)
    delete(buildURI("DELETE"), BooleanResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def getFileStatus(implicit webhdfsPath: String): Try[FileStatus] = {
    getAndConsume(buildURI("GETFILESTATUS"), FileStatusResponseHandler())
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def listStatus(implicit webhdfsPath: String): Try[List[String]] = {
    getAndConsume(buildURI("LISTSTATUS"), ListStatusResponseHandler(webhdfsPath))
  }

  @throws[ClientProtocolException]
  @throws[IOException]
  def getFileChecksum(implicit webhdfsPath: String): Try[HdfsFileChecksum] = {
    getAndConsume(buildURI("GETFILECHECKSUM"), ChecksumResponseHandler())
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
                case 401 | 404 =>
                  logger.info("_message=\"executed method: {}\" code={} reason={} args=\"{}\"", method.getName, e.statusCode.toString, e.message, args)
                case 403 =>
                  logger.info("_message=\"executed method: {}\" forbidden:\"{}\"", method.getName, args)
                  handleForbidden()
                case 500 =>
                  logger.warn("_message=\"{} failed\" responseCode={} responseMessage={} attempts={} args={}", method.getName, statusCode.toString, e.message, attempts.toString, args)
                  SleepUninterrupted(SLEEP_TIME, attempts)
                  attempts -= 1
              }
          }
        }
        catch {
          case e: Throwable => {
            exceptionReason = e.getMessage
            logger.warn("_message=\"{} failed with exception:\"", method.getName, e)
            SleepUninterrupted(SLEEP_TIME, attempts)
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
}

