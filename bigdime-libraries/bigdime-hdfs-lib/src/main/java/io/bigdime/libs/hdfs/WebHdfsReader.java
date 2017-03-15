/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import io.bigdime.core.commons.StringHelper;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

/**
 * This component can be used to read from hdfs.
 *
 * @author Neeraj Jain
 */

@Component
@Scope("prototype")
public class WebHdfsReader {
  private static final Logger logger = LoggerFactory.getLogger(WebHdfsReader.class);

  public static final String FORWARD_SLASH = "/";
  private static final String WEBHDFS_PREFIX = "/webhdfs/v1";

  @Value("${hdfs_hosts}")
  private String hostNames;

  @Value("${hdfs_port}")
  private int port;

  @Value("${hdfs_user}")
  private String hdfsUser;

  @Value("${webhdfs.auth.choice:kerberos}")
  private String authChoice;

  @Value("${webhdfs_max_attempts:5}")
  private short maxAttempts;

  HDFS_AUTH_OPTION authOption;

  private WebHdfs webHdfsForInputStream = null;

  public WebHdfsReader() {
  }

  @PostConstruct
  public void init() throws Exception {
    logger.info("PostConstruct: authChoice={} authOption={}", authChoice, authOption);
    authOption = HDFS_AUTH_OPTION.getByName(authChoice);
    logger.info("post PostConstruct: authChoice={} authOption={}", authChoice, authOption);
  }

  public WebHdfsReader(String _hostNames, int _port, String _hdfsUser, final HDFS_AUTH_OPTION _authOption) {
    hostNames = _hostNames;
    port = _port;
    hdfsUser = _hdfsUser;
    authOption = _authOption;
  }

  public InputStream getInputStream(String hdfsFilePath) throws IOException, WebHdfsException {
    if (StringHelper.isBlank(hdfsFilePath))
      throw new IllegalArgumentException("invalid filePath: empty or null");
    String webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath);
    try {
      final WebhdfsFacade facade = new WebhdfsFacade(hostNames, port, authOption);
      final Method method = WebhdfsFacade.class.getMethod("open", String.class);
      return facade.<InputStream>invokeWithRetry(method, maxAttempts, webhdfsFilePath);
    } catch (NoSuchMethodException | SecurityException e) {
      logger.error("method not found", e);
      releaseWebHdfsForInputStream();
      throw new WebHdfsException("method not found", e);
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
  public List<String> list(final String hdfsFilePath, boolean recursive) throws IOException, WebHdfsException {
    List<String> filesInDir = new ArrayList<>();
    if (StringHelper.isBlank(hdfsFilePath))
      throw new IllegalArgumentException("invalid hdfspath: empty or null");

    String webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath);

    String fileType = getFileType(webhdfsFilePath);
    if (!fileType.equalsIgnoreCase("DIRECTORY")) {
      logger.debug("_message=\"processing WebHdfsReader\" webhdfsFilePath={} represents a file", webhdfsFilePath);
      return null;
    }
    try {
      final WebhdfsFacade facade = new WebhdfsFacade(hostNames, port, authOption);
      final Method method = WebhdfsFacade.class.getMethod("listStatus", String.class);
      List<String> fileStatuses = facade.<List<String>>invokeWithRetry(method, maxAttempts, webhdfsFilePath);
      return fileStatuses;
    } catch (NoSuchMethodException | SecurityException e) {
      logger.error("method not found", e);
      throw new WebHdfsException("method not found", e);
    }
  }

  protected boolean isEmptyFile(final FileStatus fs, final String webhdfsFilePath) {
    String webhdfsFilePathWithName = webhdfsFilePath + fs.getPathSuffix();
    if (fs.getType().equals("FILE") && fs.getLength() > 0) {
      return false;// isEmptyGzFile(webhdfsFilePathWithName);
    } else {
      logger.info("_message=\"empty file found\" webhdfsFilePathWithName={}", webhdfsFilePathWithName);
      return true;
    }
  }

  protected boolean isEmptyGzFile(final String webhdfsFilePathWithName) {
    try (ZipInputStream zis = new ZipInputStream(getInputStream(webhdfsFilePathWithName))) {
      if (webhdfsFilePathWithName.endsWith(".gz")) {
        if (zis.getNextEntry() == null) {
          logger.info("_message=\"empty zip file found\" webhdfsFilePathWithName={}",
                  webhdfsFilePathWithName);
          return true;
        }
      }
    } catch (IOException | WebHdfsException e) {
      logger.error("unable to determine file size", e);
      return false;
    } finally {
      try {
        releaseWebHdfsForInputStream();
      } catch (IOException | WebHdfsException e) {
        logger.error("unable to close stream", e);
      }
    }
    return false;
  }

  /**
   * Check to see whether the file exists or not.
   *
   * @param hdfsFilePath absolute filepath
   * @return "FILE" if the file is
   * @throws WebHDFSSinkException
   */
  public String getFileType(final String hdfsFilePath) throws IOException, WebHdfsException {
    return getFileStatus(hdfsFilePath).getType();
  }

  public long getFileLength(final String hdfsFilePath) throws IOException, WebHdfsException {
    return getFileStatus(hdfsFilePath).getLength();
  }

  public FileStatus getFileStatus(final String directoryPath, final String fileName)
          throws IOException, WebHdfsException {
    String webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(directoryPath);
    webhdfsFilePath = webhdfsFilePath + fileName;
    webhdfsFilePath = appendSlash(webhdfsFilePath);
    return getFileStatus(webhdfsFilePath);
  }

  public FileStatus getFileStatusWithoutRetry(final String hdfsFilePath) throws IOException, WebHdfsException {
    return getFileStatus(hdfsFilePath, 1);
  }

  public FileStatus getFileStatus(final String hdfsFilePath) throws IOException, WebHdfsException {
    return getFileStatus(hdfsFilePath, maxAttempts);
  }

  public FileStatus getFileStatus(final String hdfsFilePath, int attempts) throws IOException, WebHdfsException {
    if (StringHelper.isBlank(hdfsFilePath))
      throw new IllegalArgumentException("invalid filePath: empty or null");

    String webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath);
    try {
      final WebhdfsFacade facade = new WebhdfsFacade(hostNames, port, authOption);
      final Method method = WebhdfsFacade.class.getMethod("getFileStatus", String.class);
      FileStatus fileStatus = facade.<FileStatus>invokeWithRetry(method, maxAttempts, webhdfsFilePath);
      return fileStatus;

    } catch (NoSuchMethodException | SecurityException e) {
      logger.error("method not found", e);
      throw new WebHdfsException("method not found", e);
    }
  }

  private WebHdfsListStatusResponse parseJson(final InputStream stream) throws JsonProcessingException, IOException {
    final ObjectMapper mapper = new ObjectMapper();
    WebHdfsListStatusResponse fs = mapper.readValue(stream, WebHdfsListStatusResponse.class);
    return fs;
  }

  private WebHdfs getWebHdfs() {
    return WebHdfsFactory.getWebHdfs(hostNames, port, hdfsUser, authOption);
  }

  public void releaseWebHdfs(WebHdfs webHdfs) {
    if (webHdfs != null)
      webHdfs.releaseConnection();
  }

  public String appendSlash(final String hdfsPath) {
    if (!StringHelper.isBlank(hdfsPath) && !hdfsPath.endsWith(FORWARD_SLASH))
      return hdfsPath + FORWARD_SLASH;
    return hdfsPath;
  }

  public String prependWebhdfsPrefixAndAppendSlash(final String hdfsPathWithoutPrefix) {
    String hdfsPath = hdfsPathWithoutPrefix;
    if (!StringHelper.isBlank(hdfsPath) && !hdfsPath.startsWith(WEBHDFS_PREFIX)) {
      hdfsPath = WEBHDFS_PREFIX + hdfsPath;
    }
    return appendSlash(hdfsPath);
  }

  /**
   * Uses "OPEN" operation and returns the InputStream to read the file
   * contents.
   *
   * @return
   * @throws IOException
   * @throws WebHdfsException
   */
  public void releaseWebHdfsForInputStream() throws IOException, WebHdfsException {
    if (webHdfsForInputStream != null)
      webHdfsForInputStream.releaseConnection();
  }

  public String getHostNames() {
    return hostNames;
  }

  public int getPort() {
    return port;
  }

  public String getHdfsUser() {
    return hdfsUser;
  }

  public String getAuthChoice() {
    return authChoice;
  }

  public short getMaxAttempts() {
    return maxAttempts;
  }

  public HDFS_AUTH_OPTION getAuthOption() {
    return authOption;
  }

  @Override
  public String toString() {
    return "WebHdfsReader [hostNames=" + hostNames + ", port=" + port + ", hdfsUser=" + hdfsUser + ", authChoice="
            + authChoice + ", maxAttempts=" + maxAttempts + ", authOption=" + authOption + "]";
  }

}
