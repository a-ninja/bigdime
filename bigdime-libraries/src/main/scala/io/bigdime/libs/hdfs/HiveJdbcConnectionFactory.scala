package io.bigdime.libs.hdfs

import java.io.IOException
import java.sql.{Connection, SQLException}

import org.apache.commons.dbcp.BasicDataSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import scala.collection.JavaConversions._

/**
  * Created by neejain on 2/14/17.
  */

case class HiveConnectionParam(hdfsAuthOption: HDFS_AUTH_OPTION, driverClassName: String, jdbcUrl: String, userName: String, password: String, hiveConfigurations: java.util.Map[String, String])

@Component
@Scope("prototype")
class HiveJdbcConnectionFactory {
  private val logger = LoggerFactory.getLogger(classOf[HiveJdbcConnectionFactory])
  private val SEMI_COLON = ";"
  private val QUESTION_MARK = "?"

  @throws[IOException]
  @throws[SQLException]
  @throws[ClassNotFoundException]
  def getConnection(connectionParam: HiveConnectionParam): Connection = {
    if (connectionParam.hdfsAuthOption eq HDFS_AUTH_OPTION.KERBEROS) {
      val con = getConnectionWithKerberosAuthentication(connectionParam.driverClassName, connectionParam.jdbcUrl, connectionParam.userName, connectionParam.password, connectionParam.hiveConfigurations)
      logger.info("connected to db using kerberos")
      con
    }
    else if (connectionParam.hdfsAuthOption eq HDFS_AUTH_OPTION.PASSWORD) {
      val con = getConnection(connectionParam.driverClassName, connectionParam.jdbcUrl, connectionParam.userName, connectionParam.password, connectionParam.hiveConfigurations)
      logger.info("connected to db using password")
      con
    } else throw new IllegalArgumentException("Unknown HDFS_AUTH_OPTION:" + connectionParam.hdfsAuthOption)
  }

  @throws[IOException]
  @throws[SQLException]
  @throws[ClassNotFoundException]
  protected def getConnectionWithKerberosAuthentication(driverClassName: String, jdbcUrl: String, keytabUser: String, keytabPath: String, hiveConfigurations: java.util.Map[String, String]): Connection = {
    val conf = new Configuration
    conf.set("hadoop.security.authentication", "Kerberos")
    getConnectionWithKerberosAuthentication(driverClassName, jdbcUrl, conf, keytabUser, keytabPath, hiveConfigurations)
  }

  @throws[IOException]
  @throws[SQLException]
  @throws[ClassNotFoundException]
  protected def getConnectionWithKerberosAuthentication(driverClassName: String, jdbcUrl: String, conf: Configuration, keytabUser: String, keytabPath: String, hiveConfigurations: java.util.Map[String, String]): Connection = getConnection0(driverClassName, jdbcUrl, conf, keytabUser, keytabPath, hiveConfigurations)

  @throws[IOException]
  @throws[SQLException]
  @throws[ClassNotFoundException]
  private def getConnection0(driverClassName: String, jdbcUrl: String, conf: Configuration, keytabUser: String, keytabPath: String, hiveConfigurations: java.util.Map[String, String]) = {
    var jdbcUrlWithConf = jdbcUrl
    val hiveConfVars = new StringBuilder

    for (key <- hiveConfigurations.keySet) {
      hiveConfVars.append(key).append("=").append(hiveConfigurations.get(key)).append(SEMI_COLON)
    }
    if (hiveConfVars.nonEmpty) jdbcUrlWithConf = jdbcUrl + QUESTION_MARK + hiveConfVars.toString
    logger.debug("connecting to db, using kerberos auth", "jdbcUrl=\"{}\" driverClassName=\"{}\" keytabUser=\"{}\" keytabPath=\"{}\"", jdbcUrlWithConf, driverClassName, keytabUser, keytabPath)
    val datasource = new BasicDataSource
    datasource.setDriverClassName(driverClassName)
    datasource.setUrl(jdbcUrlWithConf)
    loginUserFromKeytab(conf, keytabUser, keytabPath)
    datasource.getConnection
  }

  @throws[IOException]
  private def loginUserFromKeytab(conf: Configuration, keytabUser: String, keytabPath: String) {
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(keytabUser, keytabPath)
  }

  @throws[IOException]
  @throws[SQLException]
  protected def getConnection(driverClassName: String, jdbcUrl: String, username: String, password: String, hiveConfigurations: java.util.Map[String, String]): Connection = getConnection0(driverClassName, jdbcUrl, username, password, hiveConfigurations)

  @throws[IOException]
  @throws[SQLException]
  private def getConnection0(driverClassName: String, jdbcUrl: String, username: String, password: String, hiveConfigurations: java.util.Map[String, String]) = {
    var jdbcUrlWithConf = jdbcUrl
    val hiveConfVars = new StringBuilder
    for (key <- hiveConfigurations.keySet) {
      hiveConfVars.append(key).append("=").append(hiveConfigurations.get(key)).append(SEMI_COLON)
    }
    if (hiveConfVars.nonEmpty) jdbcUrlWithConf = jdbcUrl + QUESTION_MARK + hiveConfVars.toString
    logger.debug("connecting to db, using username and password", "jdbcUrl=\"{}\" driverClassName=\"{}\" username=\"{}\" password=\"{}\"", jdbcUrlWithConf, driverClassName, username, "*****")
    val datasource = new BasicDataSource
    datasource.setDriverClassName(driverClassName)
    datasource.setUrl(jdbcUrlWithConf)
    datasource.setUsername(username)
    datasource.setPassword(password)
    datasource.getConnection
  }
}
