package io.bigdime.libs.hdfs

import org.apache.commons.lang3.StringUtils

/**
  * Created by neejain on 3/7/17.
  */
object WebHdfsFactory {
  def getWebHdfs(host: String, port: Int, hdfsUser: String, authOption: HDFS_AUTH_OPTION): WebHdfs = {
    Option(authOption) match {
      case Some(HDFS_AUTH_OPTION.KERBEROS) => WebHdfsWithKerberosAuth.getInstance(host, port).addHeader(WebHDFSConstants.CONTENT_TYPE, WebHDFSConstants.APPLICATION_OCTET_STREAM)
      case Some(HDFS_AUTH_OPTION.PASSWORD) =>
        val webHdfs = WebHdfs.getInstance(host, port).addHeader(WebHDFSConstants.CONTENT_TYPE, WebHDFSConstants.APPLICATION_OCTET_STREAM)
        if (!StringUtils.isBlank(hdfsUser)) webHdfs.addParameter(WebHDFSConstants.USER_NAME, hdfsUser)
        webHdfs
      case _ => throw new IllegalArgumentException("Unknown auth option: " + authOption)
    }
  }
}
