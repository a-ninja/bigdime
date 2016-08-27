/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.http.HttpResponse;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bigdime.core.commons.StringHelper;

/**
 * This component can be used to read from hdfs.
 * 
 * @author Neeraj Jain
 *
 */

public class WebHdfsReader {
	private static final Logger logger = LoggerFactory.getLogger(WebHdfsReader.class);

	public static final String FORWARD_SLASH = "/";
	private static final String WEBHDFS_PREFIX = "/webhdfs/v1";

	private String hostNames;
	private int port;
	private String hdfsUser;
	final HDFS_AUTH_OPTION authOption;
	private WebHdfs webHdfsForInputStream = null;
	private static short DEFAULT_MAX_ATTEMPTS = 5;
	private short maxAttempts;

	public WebHdfsReader(String _hostNames, int _port, String _hdfsUser, final HDFS_AUTH_OPTION _authOption,
			short _maxAttempts) {
		this(_hostNames, _port, _hdfsUser, _authOption);
		this.maxAttempts = _maxAttempts;
	}

	public WebHdfsReader(String _hostNames, int _port, String _hdfsUser, final HDFS_AUTH_OPTION _authOption) {
		hostNames = _hostNames;
		port = _port;
		hdfsUser = _hdfsUser;
		authOption = _authOption;
		this.maxAttempts = DEFAULT_MAX_ATTEMPTS;
	}

	public InputStream getInputStream(String hdfsFilePath) throws IOException, WebHdfsException {
		if (StringHelper.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid filePath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath);

		try {
			webHdfsForInputStream = getWebHdfs();
			webHdfsForInputStream.addParameter("buffersize", "1048576");
			Method method = WebHdfs.class.getMethod("openFile", String.class);
			HttpResponse response = webHdfsForInputStream.invokeWithRetry(method, maxAttempts, webhdfsFilePath);
			return response.getEntity().getContent();
		} catch (NoSuchMethodException | SecurityException e) {
			logger.error("method not found", e);
			releaseWebHdfsForInputStream();
			throw new WebHdfsException("method not found", e);
		}
	}

	/**
	 * Uses "LISTSTATUS" operation to list the directory contents and returns
	 * the files only.
	 * 
	 * Returns an array of strings naming the files(no directories) in the
	 * directory denoted by this abstract pathname. If this abstract pathname
	 * does not denote a directory, then this method returns null. Otherwise an
	 * array of strings is returned, one for each file in the directory. Names
	 * denoting the directory itself and the directory's parent directory are
	 * not included in the result. Each string is a complete path.
	 * 
	 * There is no guarantee that the name strings in the resulting array will
	 * appear in any specific order; they are not, in particular, guaranteed to
	 * appear in alphabetical order.
	 * 
	 * @param webHdfs
	 * @param hdfsFilePath
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
		WebHdfs webHdfs = null;
		try {
			webHdfs = getWebHdfs();
			Method method = WebHdfs.class.getMethod("listStatus", String.class);
			HttpResponse response = webHdfs.invokeWithRetry(method, maxAttempts, webhdfsFilePath);
			WebHdfsListStatusResponse fss = parseJson(response.getEntity().getContent());
			List<FileStatus> fileStatuses = fss.getFileStatuses().getFileStatus();

			for (FileStatus fs : fileStatuses) {
				if (!isEmptyFile(fs, webhdfsFilePath)) {
					filesInDir.add(webhdfsFilePath + fs.getPathSuffix());
				}
				if (recursive && fs.getType().equals("DIRECTORY")) {
					filesInDir.addAll(list(webhdfsFilePath + fs.getPathSuffix(), recursive));
				}
			}
			return filesInDir;
		} catch (NoSuchMethodException | SecurityException e) {
			logger.error("method not found", e);
			throw new WebHdfsException("method not found", e);
		} finally {
			releaseWebHdfs(webHdfs);
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
	 * @param hdfsFilePath
	 *            absolute filepath
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

	public FileStatus getFileStatus(final String hdfsFilePath) throws IOException, WebHdfsException {
		if (StringHelper.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid filePath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefixAndAppendSlash(hdfsFilePath);

		WebHdfs webHdfs = null;
		try {
			webHdfs = getWebHdfs();
			Method method = WebHdfs.class.getMethod("fileStatus", String.class);
			HttpResponse response = webHdfs.invokeWithRetry(method, maxAttempts, webhdfsFilePath);
			final ObjectMapper mapper = new ObjectMapper();
			WebHdfsGetFileStatusResponse fs = mapper.readValue(response.getEntity().getContent(),
					WebHdfsGetFileStatusResponse.class);
			return fs.getFileStatus();
		} catch (NoSuchMethodException | SecurityException e) {
			logger.error("method not found", e);
			throw new WebHdfsException("method not found", e);
		} finally {
			releaseWebHdfs(webHdfs);
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
	 * @param webHdfs
	 * @param filePath
	 * @return
	 * @throws IOException
	 * @throws WebHdfsException
	 */
	public void releaseWebHdfsForInputStream() throws IOException, WebHdfsException {
		if (webHdfsForInputStream != null)
			webHdfsForInputStream.releaseConnection();
	}

}
