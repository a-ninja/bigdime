/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.libs.hdfs.WebHDFSConstants;
import io.bigdime.validation.common.AbstractValidator;

@Factory(id = "record_count_hdfs", type = HdfsRecordCountValidator.class)
@Component
@Scope("prototype")

/**
 * RecordCountValidator class is to perform whether hdfs record count from webhdfs matches source record count or not
 * 
 * record count mismatches --- validation fail
 * otherwise, validation success
 * 
 * @author Rita Liu
 */
public class HdfsRecordCountValidator implements Validator {

	private WebHdfs webHdfs;

	private static final Logger logger = LoggerFactory.getLogger(HdfsRecordCountValidator.class);
	
	private String name;
	

	/**
	 * This validate method will compare Hdfs record count and source record
	 * count
	 * 
	 * @param actionEvent
	 * @return true if Hdfs record count is same as source record count,
	 *         otherwise return false
	 * 
	 */

	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {
		ValidationResponse validationPassed = new ValidationResponse();
		AbstractValidator commonCheckValidator = new AbstractValidator();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		String host = actionEvent.getHeaders().get(ActionEventHeaderConstants.HOST_NAMES);
		String portString = actionEvent.getHeaders().get(ActionEventHeaderConstants.PORT);
		String userName = actionEvent.getHeaders().get(ActionEventHeaderConstants.USER_NAME);
		String srcRCString = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_RECORD_COUNT);
		String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hdfsFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_FILE_NAME);
		String hivePartitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
		
		
		String partitionPath = "";
		String hdfsCompletedPath = "";
		int sourceRecordCount = 0;

		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HOST_NAMES, host);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.PORT, portString);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.USER_NAME, userName);

		try {
			int port = Integer.parseInt(portString);
			if (webHdfs == null) {
				webHdfs = WebHdfs.getInstance(host, port)
						.addHeader(WebHDFSConstants.CONTENT_TYPE,WebHDFSConstants.APPLICATION_OCTET_STREAM)
						.addParameter(WebHDFSConstants.USER_NAME, userName)
						.addParameter("overwrite", "false");
			}
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal port number input while parsing string to integer");
			throw new NumberFormatException();
		}
		
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, srcRCString);
		try {
			sourceRecordCount = Integer.parseInt(srcRCString);
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal source record count input while parsing string to integer");
			throw new NumberFormatException();
		}
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HDFS_PATH, hdfsBasePath);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HDFS_FILE_NAME, hdfsFileName);
		if (StringUtils.isNotBlank(hivePartitionValues)) {
			String[] partitionList = hivePartitionValues.split(DataConstants.COMMA);
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < partitionList.length; i++) {
				stringBuilder.append(partitionList[i].trim() + DataConstants.SLASH);
			}
			partitionPath = stringBuilder.toString();
			hdfsCompletedPath = hdfsBasePath + partitionPath + hdfsFileName;
		} else {
			hdfsCompletedPath = hdfsBasePath + hdfsFileName;
		}

		int hdfsRecordCount = 0;
		try {
			hdfsRecordCount = getHdfsRecordCount(hdfsCompletedPath);
		} catch (ClientProtocolException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "ClientProtocolException",
					"Exception occurred while getting hdfs record count, cause: " + e.getMessage());
		} catch (IOException ex) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "IOException",
					"Exception occurred while getting hdfs record count, cause: " + ex.getMessage());
		}
		logger.debug(AdaptorConfig.getInstance().getName(), "performing validation",
				"hdfsCompletedPath={} sourceRecordCount={} hdfsRecordCount={}", hdfsCompletedPath, sourceRecordCount,
				hdfsRecordCount);

		if (sourceRecordCount == hdfsRecordCount) {
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Record count matches",
					"Hdfs record count({}) is same as source record count({}).", hdfsRecordCount, sourceRecordCount);
			validationPassed.setValidationResult(ValidationResult.PASSED);
		} else {
			String checksumErrorFilePath = "RCError" +DataConstants.SLASH + AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName()
					+ DataConstants.SLASH + partitionPath;
			// Take out /webhdfs/v1 from hdfsBasePath
			String hdfsDir = hdfsBasePath.substring(11);
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Record count mismatches, Hdfs file moved",
					"Hdfs record count({}) is not same as source record count({}) and hdfsCompletedPath ={}.errorFilePath ={}", hdfsRecordCount,
					sourceRecordCount,hdfsCompletedPath,hdfsBasePath + checksumErrorFilePath);
			
			try {
				if (!checkErrorRecordCountDirExists(hdfsBasePath + checksumErrorFilePath)) {
					if (makeErrorRecordCountDir(hdfsBasePath + checksumErrorFilePath)) {
						moveErrorRecordCountFile(hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
					}
				} else {
					moveErrorRecordCountFile(hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
				}

			} catch (IOException e) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Exception occurs",
						"Failed to move to provided location: " + hdfsDir + checksumErrorFilePath);
			}
			validationPassed.setValidationResult(ValidationResult.FAILED);
		}

		return validationPassed;
	}

	/**
	 * This is for hdfs record count using WebHdfs
	 * 
	 * @param fileName
	 *            hdfs file name
	 * @throws IOException
	 * @throws ClientProtocolException
	 * 
	 */
	private int getHdfsRecordCount(String fileName) throws ClientProtocolException, IOException {

		HttpResponse response = webHdfs.openFile(fileName);

		InputStream responseStream = response.getEntity().getContent();

		StringWriter writer = new StringWriter();

		IOUtils.copy(responseStream, writer);

		String theString = writer.toString();

		int hdfsRecordCount = (theString.split(System.getProperty("line.separator")).length);

		webHdfs.releaseConnection();

		responseStream.close();

		writer.close();

		return hdfsRecordCount;
	}

	/**
	 * This is to check errorRecordCountDir exists or not in hdfs
	 * 
	 * @param filePath
	 * @return true if errorRecordCountDir exists else return false
	 * @throws IOException
	 */
	private boolean checkErrorRecordCountDirExists(String filePath) throws IOException {
		HttpResponse response = webHdfs.fileStatus(filePath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 404) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * This is to make error record count directory in hdfs
	 * 
	 * @param dirPath
	 * @return true if create directory successfully else return false
	 * @throws IOException
	 */
	private boolean makeErrorRecordCountDir(String dirPath) throws IOException {
		HttpResponse response = webHdfs.mkdir(dirPath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 200) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method is move errorRecordCountFile to another location in hdfs
	 * 
	 * @param source
	 *            errorRecordCountFile path
	 * @param dest
	 *            error record count directory
	 * @throws IOException
	 */
	private void moveErrorRecordCountFile(String source, String dest) throws IOException {
		webHdfs.addParameter("destination", dest);
		webHdfs.rename(source);
		webHdfs.releaseConnection();
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}