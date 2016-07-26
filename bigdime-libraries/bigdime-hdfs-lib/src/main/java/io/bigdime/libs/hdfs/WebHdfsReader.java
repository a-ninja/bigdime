/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public String prependWebhdfsPrefix(final String hdfsPathWithoutPrefix) {
		if (!StringUtils.isBlank(hdfsPathWithoutPrefix) && !hdfsPathWithoutPrefix.startsWith(WEBHDFS_PREFIX)) {
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
		if (StringUtils.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid filePath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefix(hdfsFilePath);
		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = hdfsFilePath + FORWARD_SLASH;

		logger.debug("opening file", "webhdfsFilePath={}", webhdfsFilePath);
		HttpResponse response = webHdfs.openFile(webhdfsFilePath);

		if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
			logger.debug("file opened", "responseCode={} hdfsPath={} responseMessage={}",
					response.getStatusLine().getStatusCode(), webhdfsFilePath,
					response.getStatusLine().getReasonPhrase());

		} else if (response.getStatusLine().getStatusCode() == 404) {
			logger.debug("file does not exist", "responseCode={} hdfsPath={} responseMessage={}",
					response.getStatusLine().getStatusCode(), webhdfsFilePath,
					response.getStatusLine().getReasonPhrase());
			throw new FileNotFoundException("File not found");
		} else {
			logger.warn("file existence not known, responseCode={} hdfsPath={} responseMessage={}",
					response.getStatusLine().getStatusCode(), webhdfsFilePath,
					response.getStatusLine().getReasonPhrase());
			throw new WebHdfsException("file existence not known, responseCode="
					+ response.getStatusLine().getStatusCode() + ", filePath=" + webhdfsFilePath);
		}

		return response.getEntity().getContent();
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
		if (StringUtils.isBlank(hdfsFilePath))
			throw new IllegalArgumentException("invalid hdfspath: empty or null");

		String webhdfsFilePath = prependWebhdfsPrefix(hdfsFilePath);
		if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
			webhdfsFilePath = webhdfsFilePath + FORWARD_SLASH;

		String fileType = getFileType(webHdfs, webhdfsFilePath);
		if (!fileType.equalsIgnoreCase("DIRECTORY")) {
			logger.debug("processing WebHdfsReader", "hdfsPath={} represents a file", webhdfsFilePath);
			return null;
		}

		HttpResponse response = webHdfs.listStatus(webhdfsFilePath);

		if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
			logger.debug("file exists", "responseCode={} hdfsPath={} responseMessage={}",
					response.getStatusLine().getStatusCode(), webhdfsFilePath,
					response.getStatusLine().getReasonPhrase());
			WebHdfsListStatusResponse fss = parseJson(response.getEntity().getContent());
			List<FileStatus> fileStatuses = fss.getFileStatuses().getFileStatus();

			for (FileStatus fs : fileStatuses) {
				if (fs.getType().equals("FILE")) {
					filesInDir.add(webhdfsFilePath + fs.getPathSuffix());

				}
				if (recursive && fs.getType().equals("DIRECTORY")) {
					filesInDir.addAll(list(webHdfs, webhdfsFilePath + fs.getPathSuffix(), recursive));
				}
			}

		} else if (response.getStatusLine().getStatusCode() == 404) {
			logger.debug("_message=\"file does not exist\" responseCode={} hdfsPath={} responseMessage={}",
					response.getStatusLine().getStatusCode(), webhdfsFilePath,
					response.getStatusLine().getReasonPhrase());
			throw new FileNotFoundException("File not found: filePath=" + webhdfsFilePath);
		} else {
			logger.warn("_message=\"file existence not known\", responseCode={} hdfsPath={} responseMessage={}",
					response.getStatusLine().getStatusCode(), webhdfsFilePath,
					response.getStatusLine().getReasonPhrase());
			throw new WebHdfsException("file existence not known, responseCode="
					+ response.getStatusLine().getStatusCode() + ", filePath=" + webhdfsFilePath);
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

	public FileStatus getFileStatus(WebHdfs webHdfs, final String hdfsFilePath) throws IOException, WebHdfsException {
		try {
			if (StringUtils.isBlank(hdfsFilePath))
				throw new IllegalArgumentException("invalid filePath: empty or null");

			String webhdfsFilePath = prependWebhdfsPrefix(hdfsFilePath);
			if (!webhdfsFilePath.endsWith(FORWARD_SLASH))
				webhdfsFilePath = hdfsFilePath + FORWARD_SLASH;

			HttpResponse response = webHdfs.fileStatus(webhdfsFilePath);
			if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
				logger.debug("file exists", "responseCode={} filePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), webhdfsFilePath,
						response.getStatusLine().getReasonPhrase());
				final ObjectMapper mapper = new ObjectMapper();
				WebHdfsGetFileStatusResponse fs = mapper.readValue(response.getEntity().getContent(),
						WebHdfsGetFileStatusResponse.class);
				return fs.getFileStatus();
			} else if (response.getStatusLine().getStatusCode() == 404) {
				logger.debug("file does not exist", "responseCode={} filePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), webhdfsFilePath,
						response.getStatusLine().getReasonPhrase());
				throw new FileNotFoundException("File not found");
			} else {
				logger.warn("file existence not known, responseCode={} filePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), webhdfsFilePath,
						response.getStatusLine().getReasonPhrase());
				throw new WebHdfsException("file existence not known, responseCode="
						+ response.getStatusLine().getStatusCode() + ", filePath=" + webhdfsFilePath);
			}
		} catch (Exception e) {
			logger.warn("file creation", "_message=\"Unable to check the status of the file:\" retry={} error={}", e);
			throw new WebHdfsException("could not get the file status", e);
		}
	}
}
