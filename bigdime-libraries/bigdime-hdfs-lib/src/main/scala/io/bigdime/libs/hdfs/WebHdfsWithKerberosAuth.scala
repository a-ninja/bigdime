package io.bigdime.libs.hdfs

import java.io.IOException
import java.net.URI

import org.apache.http.HttpResponse
import org.apache.http.auth.{AuthSchemeProvider, AuthScope, Credentials}
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.config.AuthSchemes
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.config.RegistryBuilder
import org.apache.http.impl.auth.SPNegoSchemeFactory
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClientBuilder}
import org.slf4j.LoggerFactory

/**
  * Created by neejain on 1/31/17.
  */

object WebHdfsWithKerberosAuth {
  private val logger = LoggerFactory.getLogger(classOf[WebHdfsWithKerberosAuth])
  private val DEFAULT_KRB5_CONFIG_LOCATION = "/etc/krb5.conf"
  private val DEFAULT_LOGIN_CONFIG_LOCATION = "/opt/bigdime/login.conf"

  def getInstance(host: String, port: Int) = new WebHdfsWithKerberosAuth(host, port)
}

protected case class WebHdfsWithKerberosAuth(h: String, p: Int) extends WebHdfs(h, p) {

  import WebHdfsWithKerberosAuth._

  private var activeHost: String = null

  override protected def rotateHost: String = {
    logger.debug("_message=\"rotating host\" hosts={} current_active_host={}", host: Any, activeHost: Any)
    activeHost = ActiveNameNodeResolver.rotateHost(host, activeHost)
    logger.info("_message=\"rotated host\" hosts={} new_active_host={}", host: Any, activeHost: Any)
    activeHost
  }

  def loadConfigFiles = {
    var krb5ConfigPath = System.getProperty("java.security.krb5.conf")
    if (krb5ConfigPath == null) krb5ConfigPath = WebHdfsWithKerberosAuth.DEFAULT_KRB5_CONFIG_LOCATION
    var loginConfigPath = System.getProperty("java.security.auth.login.config")
    if (loginConfigPath == null) loginConfigPath = WebHdfsWithKerberosAuth.DEFAULT_LOGIN_CONFIG_LOCATION
    logger.debug("krb5ConfigPath={} loginConfigPath={}", krb5ConfigPath: Any, loginConfigPath: Any)
    (krb5ConfigPath, loginConfigPath)
  }

  override protected def initConnection() {
    logger.debug("initializing connection")
    val (krb5ConfigPath, loginConfigPath) = loadConfigFiles
    val skipPortAtKerberosDatabaseLookup = true
    System.setProperty("java.security.krb5.conf", krb5ConfigPath)
    System.setProperty("sun.security.krb5.debug", "false")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    System.setProperty("java.security.auth.login.config", loginConfigPath)
    val authSchemeRegistry = RegistryBuilder.create[AuthSchemeProvider].register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(skipPortAtKerberosDatabaseLookup)).build
    try {
      if (activeHost == null) rotateHost
      val uri = new URI(activeHost)

      if (uri.getScheme.equalsIgnoreCase("https")) {
        connMgr = getConnectionManagerWithDefaultSSL
        setHttpClient(HttpClientBuilder.create.setConnectionManager(connMgr).setDefaultAuthSchemeRegistry(authSchemeRegistry).build)
      }
      else setHttpClient(HttpClientBuilder.create.setDefaultAuthSchemeRegistry(authSchemeRegistry).build)


      //      if (uri.getScheme.equalsIgnoreCase("https")) {
      //        connMgr = getConnectionManagerWithDefaultSSL
      //        setHttpClient(HttpClientBuilder.create.setConnectionManager(connMgr).setDefaultAuthSchemeRegistry(authSchemeRegistry).build)
      //      }
      //      else setHttpClient(HttpClientBuilder.create.setDefaultAuthSchemeRegistry(authSchemeRegistry).build)
      roundRobinStrategy.setHosts(activeHost)
    }
    catch {
      case e: Exception => {
        logger.warn("_message=\"{} failed to create httpClient\" ", e)
      }
    }
  }

  // LISTSTATUS, OPEN, GETFILESTATUS, GETCHECKSUM,
  @throws[ClientProtocolException]
  @throws[IOException]
  override protected def get: HttpResponse = {
    logger.debug("WebHdfsWithKerberosAuth getting")
    val context = HttpClientContext.create
    val credentialsProvider = new BasicCredentialsProvider
    val useJaasCreds = new Credentials() {
      def getPassword = null

      def getUserPrincipal = null
    }
    credentialsProvider.setCredentials(new AuthScope(null, -1, null), useJaasCreds)
    context.setCredentialsProvider(credentialsProvider)
    // this.addParameter("anonymous=true", "true");
    logger.debug("WebHdfsWithKerberosAuth getting from:{}", uri)
    httpRequest = new HttpGet(uri)
    logger.debug("HTTP request: {}", httpRequest.getURI)
    uri = null
    httpClient.execute(httpRequest, context)
  }
}
