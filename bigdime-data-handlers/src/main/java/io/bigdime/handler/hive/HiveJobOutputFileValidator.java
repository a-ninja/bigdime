package io.bigdime.handler.hive;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsReader;

@Component
@Scope("prototype")
public class HiveJobOutputFileValidator {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(HiveJobOutputFileValidator.class));
	@Autowired
	private WebHdfsReader webHdfsReader;

	/**
	 * Check if the file/directory specified by filePath exists in HDFS.
	 * 
	 * @param webHdfsReader
	 *            component that actually invokes webhdfs REST API.
	 * @param filePath
	 *            absolute hdfs path, without /webhdfs/{version} prefix
	 * @return true if the file exists, false otherwise
	 * @throws IOException
	 * @throws WebHdfsException
	 */
	public boolean validateOutputFile(String filePath) throws IOException, WebHdfsException {
		try {
			return (webHdfsReader.getFileStatus(filePath) != null);
		} catch (WebHdfsException ex) {
			logger.warn("validateOutputFile", "_message=\"unable to get file status\" filePath={} error={}", filePath,
					ex.getMessage(), ex);
			if (ex.getStatusCode() == 404) {
				logger.info("validateOutputFile",
						"_message=\"file not found in hdfs, returning false\" filePath={} error={}", filePath,
						ex.getMessage());
				return false;
			} else {
				throw ex;
			}
		}
	}
}
