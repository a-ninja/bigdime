package io.bigdime.libs.hdfs

import java.io.{IOException, InputStream}
import java.lang.reflect.Method
import java.net.{URI, URISyntaxException}
import java.security.cert.{CertificateException, X509Certificate}
import java.security.{KeyManagementException, KeyStoreException, NoSuchAlgorithmException}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import io.bigdime.util.TryAndGiveUp
import org.apache.http.auth.{AuthSchemeProvider, AuthScope, Credentials}
import org.apache.http.client.config.AuthSchemes
import org.apache.http.client.methods.{HttpUriRequest, _}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.{ClientProtocolException, ResponseHandler}
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.HttpClientConnectionManager
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.auth.SPNegoSchemeFactory
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.ssl.{SSLContexts, TrustStrategy}
import org.apache.http.{Header, HttpResponse}

import scala.util.{Failure, Success, Try}

/**
  * Created by neejain on 3/7/17.
  */
trait AbstractWebHdfsHttpClient extends LazyLogging {
  private val MAX_TOTAL = 20
  private val MAX_PER_ROUTE = 20

  /**
    * Setup the PoolingHttpClientConnectionManager instance
    */
  protected val connMgr = getConnectionManagerWithDefaultSSL

  protected val connectionMonitorThread = new IdleConnectionMonitorThread(connMgr)
  connectionMonitorThread.start()
  protected var httpClientMap = Map[String, Try[CloseableHttpClient]]()
  initConnectionPool()

  def initConnectionPool() = {
    logger.info("initializing connections")
    httpClientMap = Map[String, Try[CloseableHttpClient]](
      "https" -> Success(httpsConnection),
      "http" -> Success(httpConnection))
  }

  @throws[URISyntaxException]
  @throws[KeyManagementException]
  @throws[NoSuchAlgorithmException]
  @throws[KeyStoreException]
  private def getConnectionManagerWithDefaultSSL: HttpClientConnectionManager = {
    val sslContext = SSLContexts.custom.loadTrustMaterial(new TrustStrategy() {
      @throws[CertificateException]
      def isTrusted(chain: Array[X509Certificate], authType: String) = true
    }).build
    val socketFactoryRegistry = RegistryBuilder.create[ConnectionSocketFactory].register("https", new SSLConnectionSocketFactory(sslContext)).build
    val mgr = new PoolingHttpClientConnectionManager(socketFactoryRegistry)
    mgr.setMaxTotal(MAX_TOTAL)
    mgr.setDefaultMaxPerRoute(MAX_PER_ROUTE)
    mgr
  }


  def execute[T](scheme: String, request: HttpUriRequest, proc: ResponseHandler[T]) = {
    get(scheme).get.execute(request, proc, httpContext())
  }

  def execute(scheme: String, request: HttpUriRequest) = {
    get(scheme).get.execute(request, httpContext())
  }

  protected def get(uriScheme: String): Try[CloseableHttpClient] = {
    uriScheme match {
      case "http" | "https" => httpClientMap.get(uriScheme).getOrElse(Failure(new IllegalStateException(s"unable to find a httpclient for $uriScheme scheme")))
      case _ => Failure(new IllegalArgumentException(s"unable to find a httpclient for $uriScheme scheme"))
    }
  }

  protected def httpConnection: CloseableHttpClient

  protected def httpsConnection: CloseableHttpClient

  protected def httpContext(): HttpClientContext

  class IdleConnectionMonitorThread(val connMgr: HttpClientConnectionManager) extends Thread {
    private var shutdownFlag = false

    override def run() {
      TryAndGiveUp()(() => {
        while (!shutdownFlag) this synchronized {
          wait(5000)
          // Close expired connections
          connMgr.closeExpiredConnections()
          // Optionally, close connections
          // that have been idle longer than 30 sec
          connMgr.closeIdleConnections(180, TimeUnit.SECONDS)
        }
      })
    }

    def shutdown() {
      shutdownFlag = true
      this synchronized {
        notifyAll()
      }
    }
  }

}

object WebHdfsHttpClientFactory {
  def apply(authOption: HDFS_AUTH_OPTION): AbstractWebHdfsHttpClient = {
    Option(authOption) match {
      case Some(HDFS_AUTH_OPTION.KERBEROS) => WebHdfsHttpClientWithKerberos
      case Some(HDFS_AUTH_OPTION.PASSWORD) => WebHdfsHttpClient
      case _ => throw new IllegalArgumentException("Unknown auth option: " + authOption)
    }
  }
}

object WebHdfsHttpClient extends AbstractWebHdfsHttpClient with LazyLogging {
  protected def httpConnection: CloseableHttpClient = HttpClientBuilder.create.build()

  protected def httpsConnection: CloseableHttpClient = HttpClients.custom.setConnectionManager(connMgr).build()

  override def httpContext(): HttpClientContext = null

  logger.info("WebHdfsHttpClient {}", "constructor")
}

object WebHdfsHttpClientWithKerberos extends AbstractWebHdfsHttpClient with LazyLogging {
  private val DEFAULT_KRB5_CONFIG_LOCATION = "/etc/krb5.conf"
  private val DEFAULT_LOGIN_CONFIG_LOCATION = "/opt/bigdime/login.conf"

  val initConfig = {
    val (krb5ConfigPath, loginConfigPath) = (Option(System.getProperty("java.security.krb5.conf")).getOrElse(DEFAULT_KRB5_CONFIG_LOCATION),
      Option(System.getProperty("java.security.auth.login.config")).getOrElse(DEFAULT_LOGIN_CONFIG_LOCATION))
    System.setProperty("java.security.krb5.conf", krb5ConfigPath)
    System.setProperty("sun.security.krb5.debug", "false")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    System.setProperty("java.security.auth.login.config", loginConfigPath)
  }

  private val authSchemeRegistry = RegistryBuilder.create[AuthSchemeProvider].register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build

  protected def httpsConnection: CloseableHttpClient = HttpClientBuilder.create.setConnectionManager(connMgr).setDefaultAuthSchemeRegistry(authSchemeRegistry).build

  protected def httpConnection: CloseableHttpClient = HttpClientBuilder.create.setDefaultAuthSchemeRegistry(authSchemeRegistry).build

  override def httpContext() = {
    val context = HttpClientContext.create
    val credentialsProvider = new BasicCredentialsProvider
    val useJaasCreds = new Credentials() {
      def getPassword = null

      def getUserPrincipal = null
    }
    credentialsProvider.setCredentials(new AuthScope(null, -1, null), useJaasCreds)
    context.setCredentialsProvider(credentialsProvider)
    context
  }

  logger.info("WebHdfsHttpClientWithKerberos {}", "constructor")
}

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
    * Wrapper to call initConnection of WebHdfsHttpClient
    */
  private def initConnection() = webhdfsHttpClient.initConnectionPool()


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
  def invokeWithRetry[T](method: Method, maxAttempts: Int, args: String*): T = {
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
                  rotateHost()
                  initConnection()
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

  private def logResponse(e: WebHdfsException, message: String, attempts: Int, args: String*) {
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


