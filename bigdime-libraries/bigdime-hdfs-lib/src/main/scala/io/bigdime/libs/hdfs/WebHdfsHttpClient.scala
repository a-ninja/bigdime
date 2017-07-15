package io.bigdime.libs.hdfs

import java.net.URISyntaxException
import java.security.cert.{CertificateException, X509Certificate}
import java.security.{KeyManagementException, KeyStoreException, NoSuchAlgorithmException}
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import io.bigdime.util.TryAndGiveUp
import org.apache.http.auth.{AuthSchemeProvider, AuthScope, Credentials}
import org.apache.http.client.ResponseHandler
import org.apache.http.client.config.AuthSchemes
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.HttpClientConnectionManager
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.auth.SPNegoSchemeFactory
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.ssl.{SSLContexts, TrustStrategy}

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
  init()

  def init() = {
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
  private val DEFAULT_LOGIN_CONFIG_LOCATION = "/opt/credentials/login.conf"

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