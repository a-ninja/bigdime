/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
	private long sleepTime = 3000;

	public String prependWebhdfsPrefix(final String hdfsPathWithoutPrefix) {
		if (!StringHelper.isBlank(hdfsPathWithoutPrefix) && !hdfsPathWithoutPrefix.startsWith(WEBHDFS_PREFIX)) {
			return "/webhdfs/v1" + hdfsPathWithoutPrefix;
		}
		return hdfsPathWithoutPrefix;
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
	public InputStream getInputStream(WebHdfs webHdfs, String hdfsFilePath) throws IOException, WebHdfsException {
		if (StringHelper.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid filePath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefix(hdfsFilePath);
		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = hdfsFilePath + FORWARD_SLASH;

		String exceptionReason = null;
		int attempts = 0;
		boolean isSuccess = false;
		do {
			attempts++;
			logger.debug("_message=\"getting status of file\" hdfsFilePath={} webhdfsFilePath={} attempts={}",
					hdfsFilePath, webhdfsFilePath, attempts);
			HttpResponse response = webHdfs.openFile(webhdfsFilePath);

			if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
				logger.debug("_message=\"file opened\" responseCode={} hdfsPath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), webhdfsFilePath,
						response.getStatusLine().getReasonPhrase());
				isSuccess = true;
				return response.getEntity().getContent();

			} else {
				exceptionReason = logResponse(response, "getInputStream Failed", attempts, hdfsFilePath,
						webhdfsFilePath);
			}
		} while (!isSuccess && attempts < 3);
		if (!isSuccess) {
			logger.error("_message=\"getInputStream failed After 3 retries :\"");
			throw new WebHDFSSinkException(exceptionReason);
		}

		return null;
	}

	private WebHdfsListStatusResponse parseJson(final InputStream stream) throws JsonProcessingException, IOException {
		final ObjectMapper mapper = new ObjectMapper();
		WebHdfsListStatusResponse fs = mapper.readValue(stream, WebHdfsListStatusResponse.class);
		return fs;
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
	 * appear in alphabetical order. The webhdfs connection is left open. Caller
	 * must release the connection.
	 * 
	 * @param webHdfs
	 * @param hdfsFilePath
	 * @return
	 * @throws WebHdfsException
	 * @throws IOException
	 * @throws Exception
	 */
	public List<String> list(final WebHdfs webHdfs, final String hdfsFilePath, boolean recursive)
			throws IOException, WebHdfsException {
		List<String> filesInDir = new ArrayList<>();
		if (StringHelper.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid hdfspath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefix(hdfsFilePath);
		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = webhdfsFilePath + FORWARD_SLASH;

		String fileType = getFileType(webHdfs, webhdfsFilePath);
		if (!fileType.equalsIgnoreCase("DIRECTORY")) {
			logger.debug("_message=\"processing WebHdfsReader\" webhdfsFilePath={} represents a file", webhdfsFilePath);
			return null;
		}

		boolean isSuccess = false;
		int attempts = 0;
		String exceptionReason = null;

		do {
			attempts++;
			logger.debug("_message=\"getting status of file\" hdfsFilePath={} webhdfsFilePath={} attempts={}",
					hdfsFilePath, webhdfsFilePath, attempts);
			HttpResponse response = webHdfs.listStatus(webhdfsFilePath);
			if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
				logger.debug(
						"_message=\"file exists\" responseCode={} hdfsFilePath={} webhdfsFilePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), hdfsFilePath, webhdfsFilePath,
						response.getStatusLine().getReasonPhrase());
				isSuccess = true;

				WebHdfsListStatusResponse fss = parseJson(response.getEntity().getContent());
				List<FileStatus> fileStatuses = fss.getFileStatuses().getFileStatus();

				for (FileStatus fs : fileStatuses) {
					if (fs.getType().equals("FILE") && fs.getLength() > 0) {
						filesInDir.add(webhdfsFilePath + fs.getPathSuffix());
					}
					if (recursive && fs.getType().equals("DIRECTORY")) {
						filesInDir.addAll(list(webHdfs, webhdfsFilePath + fs.getPathSuffix(), recursive));
					}
				}
				return filesInDir;
			} else {
				exceptionReason = logResponse(response, "list Failed", attempts, hdfsFilePath, webhdfsFilePath);
			}
		} while (!isSuccess && attempts < 3);

		if (!isSuccess) {
			logger.error("_message=\"getInputStream failed After 3 retries :\"");
			throw new WebHDFSSinkException(exceptionReason);
		}
		return filesInDir;
	}

	/**
	 * Check to see whether the file exists or not.
	 * 
	 * @param hdfsFilePath
	 *            absolute filepath
	 * @return "FILE" if the file is
	 * @throws WebHDFSSinkException
	 */
	public String getFileType(WebHdfs webHdfs, final String hdfsFilePath) throws IOException, WebHdfsException {
		return getFileStatus(webHdfs, hdfsFilePath).getType();
	}

	public long getFileLength(WebHdfs webHdfs, final String hdfsFilePath) throws IOException, WebHdfsException {
		return getFileStatus(webHdfs, hdfsFilePath).getLength();
	}

	public FileStatus getFileStatus(WebHdfs webHdfs, final String directoryPath, final String fileName)
			throws IOException, WebHdfsException {
		String webhdfsFilePath = prependWebhdfsPrefix(directoryPath);
		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = directoryPath + FORWARD_SLASH;
		webhdfsFilePath = webhdfsFilePath + fileName;

		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = webhdfsFilePath + FORWARD_SLASH;

		return getFileStatus(webHdfs, webhdfsFilePath);

	}

	public FileStatus getFileStatus(WebHdfs webHdfs, final String hdfsFilePath) throws IOException, WebHdfsException {
		if (StringHelper.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid filePath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefix(hdfsFilePath);
		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = hdfsFilePath + FORWARD_SLASH;

		boolean isSuccess = false;
		int attempts = 0;
		String exceptionReason = null;
		do {
			try {
				attempts++;
				logger.debug("_message=\"getting status of file\" hdfsFilePath={} webhdfsFilePath={} attempts={}",
						hdfsFilePath, webhdfsFilePath, attempts);
				HttpResponse response = webHdfs.fileStatus(webhdfsFilePath);
				if (response.getStatusLine().getStatusCode() == 200
						|| response.getStatusLine().getStatusCode() == 201) {
					logger.debug(
							"_message=\"file exists\" responseCode={} hdfsFilePath={} webhdfsFilePath={} responseMessage={}",
							response.getStatusLine().getStatusCode(), hdfsFilePath, webhdfsFilePath,
							response.getStatusLine().getReasonPhrase());
					isSuccess = true;
					final ObjectMapper mapper = new ObjectMapper();
					WebHdfsGetFileStatusResponse fs = mapper.readValue(response.getEntity().getContent(),
							WebHdfsGetFileStatusResponse.class);
					isSuccess = true;
					return fs.getFileStatus();
				} else {
					exceptionReason = logResponse(response, "WebHdfs getFileStatus Failed", attempts, hdfsFilePath,
							webhdfsFilePath);
				}
			} catch (Exception e) {
				exceptionReason = e.getMessage();
				logger.warn("_message=\"WebHdfs getFileStatus Failed:\" attempts={} host ={} error={}", attempts,
						webHdfs.getHost(), e);
			}
		} while (!isSuccess && attempts < 3);
		if (!isSuccess) {
			logger.error("_message=\"getFileStatus failed After 3 retries :\"");
			throw new WebHDFSSinkException(exceptionReason);
		}
		return null;
	}

	private String logResponse(HttpResponse response, String message, int attempts, String hdfsFilePath,
			String webhdfsFilePath) {
		String exceptionReason = response.getStatusLine().getStatusCode() + ":"
				+ response.getStatusLine().getReasonPhrase();
		logger.warn(
				"_message=\"" + message
						+ "\" responseCode={}  responseMessage={} attempts={} attempts={} webhdfsFilePath={}",
				response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), attempts,
				hdfsFilePath, webhdfsFilePath);
		try {
			Thread.sleep(sleepTime * (attempts + 1));
		} catch (InterruptedException e) {
			logger.warn("sleep interrupted", e);
		}
		return exceptionReason;

	}
}
