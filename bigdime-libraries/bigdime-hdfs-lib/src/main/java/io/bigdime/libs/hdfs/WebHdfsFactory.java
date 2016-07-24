package io.bigdime.libs.hdfs;

import org.apache.commons.lang3.StringUtils;

/**
 * Provide instance of WebHDFS based on the authentication options. Supported
 * options as KERBEROS and PASSWORD.
 * 
 * @author Neeraj Jain
 *
 */
public class WebHdfsFactory {

	/**
	 * Get an instance of WebHdfs.
	 * 
	 * @param host
	 *            hostname for webhdfs connection
	 * @param port
	 *            webhdfs connection port
	 * @param hdfsUser
	 *            username for connecting using webhdfs. usually null if
	 *            authOption is KERBEROS.
	 * @param authOption
	 *            : KERBEROS or PASSWORD.
	 * 
	 * @return instance of WebHdfs
	 * @throws IllegalArgumentException
	 *             is authOption is null or not among KERBEROS or PASSWORD.
	 */
	public static WebHdfs getWebHdfs(String host, int port, String hdfsUser, final HDFS_AUTH_OPTION authOption) {
		WebHdfs webHdfs = null;
		if (authOption == null) {
			throw new IllegalArgumentException("Invalid auth option, null not supported");
		}

		if (authOption == HDFS_AUTH_OPTION.KERBEROS)
			webHdfs = WebHdfsWithKerberosAuth.getInstance(host, port).addHeader(WebHDFSConstants.CONTENT_TYPE,
					WebHDFSConstants.APPLICATION_OCTET_STREAM);
		else if (authOption == HDFS_AUTH_OPTION.PASSWORD) {
			webHdfs = WebHdfs.getInstance(host, port).addHeader(WebHDFSConstants.CONTENT_TYPE,
					WebHDFSConstants.APPLICATION_OCTET_STREAM);
		} else {
			throw new IllegalArgumentException("Unknown auth option: " + authOption);
		}
		if (!StringUtils.isBlank(hdfsUser)) {
			webHdfs.addParameter(WebHDFSConstants.USER_NAME, hdfsUser);
		}
		return webHdfs;
	}
}
