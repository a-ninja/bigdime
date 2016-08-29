/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataChecksum;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
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
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.core.validation.Validator;
import io.bigdime.libs.hdfs.WebHDFSConstants;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.validation.common.AbstractValidator;

@Factory(id = "raw_checksum", type = RawChecksumValidator.class)
@Component
@Scope("prototype")
/**
 * Performs validation by comparing expected(from event header) and actual
 * checksum(from actual document on disk).
 * 
 * @author Neeraj Jain, Rita Liu
 *
 */
public class RawChecksumValidator implements Validator {

	private WebHdfs webHdfs;
	private static final Logger logger = LoggerFactory.getLogger(RawChecksumValidator.class);
	private String name;

	/**
	 * This validate method will get hdfs file raw checksum based on provided
	 * parameters from actionEvent, and get source file raw checksum based on
	 * source file path, then compare them
	 * 
	 * @param actionEvent
	 * @return true if both raw checksum are same, else return false and hdfs
	 *         file move to errorChecksum location
	 *
	 */

	private boolean isReadyToValidate(final ActionEvent actionEvent) {
		String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hdfsFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_FILE_NAME);


		String totalSize = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_SIZE);
		String totalRead = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_READ);
		
		String fileReadComplete = actionEvent.getHeaders().get(ActionEventHeaderConstants.READ_COMPLETE);
		if (fileReadComplete == null || !fileReadComplete.equalsIgnoreCase("true")) {
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"processing RawChecksumValidator",
					"Raw Checksum validation being skipped, totalSize={} totalRead={} hdfsBasePath={} hdfsFileName={} fileReadComplete={}",
					totalSize, totalRead, hdfsBasePath, hdfsFileName, fileReadComplete);
			return false;
		}
		return true;
	}

	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {
		ValidationResponse validationPassed = new ValidationResponse();
		AbstractValidator commonValidator = new AbstractValidator();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		String host = actionEvent.getHeaders().get(ActionEventHeaderConstants.HOST_NAMES);
		String portString = actionEvent.getHeaders().get(ActionEventHeaderConstants.PORT);
		String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hdfsFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_FILE_NAME);
		String hivePartitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
		String sourceFilePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_PATH);
		String partitionPath = "";
		String hdfsCompletedPath = "";
		String sourceFileChecksum = "";

		commonValidator.checkNullStrings(ActionEventHeaderConstants.HOST_NAMES, host);
		commonValidator.checkNullStrings(ActionEventHeaderConstants.PORT, portString);

		if (!isReadyToValidate(actionEvent)) {
			validationPassed.setValidationResult(ValidationResult.NOT_READY);
			return validationPassed;
		}
		try {
			int port = Integer.parseInt(portString);
			if (webHdfs == null) {
				webHdfs = WebHdfs.getInstance(host, port);
			}
			webHdfs.addParameter(WebHDFSConstants.USER_NAME, "hdfs");
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal port number input while parsing string to integer");
			throw new NumberFormatException();
		}

		commonValidator.checkNullStrings(ActionEventHeaderConstants.HDFS_PATH, hdfsBasePath);
		commonValidator.checkNullStrings(ActionEventHeaderConstants.HDFS_FILE_NAME, hdfsFileName);
		commonValidator.checkNullStrings(ActionEventHeaderConstants.SOURCE_FILE_PATH, sourceFilePath);

		try {
			if (!new File(sourceFilePath).exists()) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "File Not Found",
						"Source file does not exist : " + sourceFilePath, FileNotFoundException.class);

			} else {
				sourceFileChecksum = getSourceFileChecksum(sourceFilePath);
			}
		} catch (IOException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "IOException",
					"Exception occurred while getting source file raw checksum", e);
		} catch (URISyntaxException ex) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "URISyntaxException",
					"Exception occurred while getting source file raw checksum", ex);
		}
		if (sourceFileChecksum.length() > 0) {

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

			String hdfsFileChecksum = "";
			try {
				hdfsFileChecksum = getHdfsFileChecksum(hdfsCompletedPath);
			} catch (ClientProtocolException e) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "ClientProtocolException",
						"Exception occurred while getting hdfs raw checksum, cause: " + e.getCause());
			} catch (IOException ex) {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "IOException",
						"Exception occurred while getting hdfs raw checksum, cause: " + ex.getMessage());
			}

			if (hdfsFileChecksum.length() > 0) {

				if (sourceFileChecksum.equals(hdfsFileChecksum)) {
					logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
							"Raw Checksum matches", "Hdfs file raw checksum is same as source file raw checksum.");
					validationPassed.setValidationResult(ValidationResult.PASSED);
				} else {
					
					String checksumErrorFilePath = "ChecksumError" + DataConstants.SLASH
							+ AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName() + DataConstants.SLASH + partitionPath;
					// take out /webhdfs/v1 from hdfsBasePath for building
					// checksumErrorFilePath
					String hdfsDir = hdfsBasePath.substring(11);
					try {
						if (!checkErrorChecksumDirExists(hdfsBasePath + checksumErrorFilePath)) {
							if (makeErrorChecksumDir(hdfsBasePath + checksumErrorFilePath)) {

								moveErrorChecksumFile(hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
							}
						} else {

							moveErrorChecksumFile(hdfsCompletedPath, hdfsDir + checksumErrorFilePath);
						}

					} catch (IOException e) {
						logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
								"Exception occurs",
								"Failed to move to provided location: " + hdfsDir + checksumErrorFilePath);
					}
					
					logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
							"Raw Checksum mismatches HDFS file moved",
							"Hdfs file raw checksum is different as source file raw checksum, hdfs file moved to {}. sourceFileChecksum={} hdfsFileChecksum={}", 
							hdfsBasePath + checksumErrorFilePath, sourceFileChecksum, hdfsFileChecksum);

					validationPassed.setValidationResult(ValidationResult.FAILED);
				}
			} else {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "WARNING",
						"Hdfs file checksum cannot be calculated.");
				validationPassed.setValidationResult(ValidationResult.FAILED);
			}
		} else {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "WARNING",
					"Source file checksum cannot be calculated.");
			validationPassed.setValidationResult(ValidationResult.FAILED);
		}
		return validationPassed;
	}

	/**
	 * This method is getting hdfs file raw checksum using WebHdfs
	 * 
	 * @param hdfsFilePath
	 * @return hdfs raw checksum string
	 * @throws ClientProtocolException
	 * @throws IOException
	 */

	private String getHdfsFileChecksum(String hdfsFilePath) throws ClientProtocolException, IOException {
		HttpResponse response = webHdfs.checksum(hdfsFilePath);
		InputStream responseStream = response.getEntity().getContent();
		StringBuilder stringBuilder = new StringBuilder();
		BufferedReader buffer = new BufferedReader(new InputStreamReader(responseStream, "UTF-8"));
		String line = null;
		while ((line = buffer.readLine()) != null) {
			stringBuilder.append(line);
		}
		responseStream.close();
		buffer.close();
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(stringBuilder.toString());
		JsonNode fileChecksum = jsonNode.get("FileChecksum");
		webHdfs.releaseConnection();
		String hdfsFileChecksum = fileChecksum.get("bytes").toString();
		hdfsFileChecksum = hdfsFileChecksum.substring(25, 57).replace("\"", "");
		return hdfsFileChecksum;
	}

	/**
	 * This method is calculating source file raw checksum
	 * 
	 * @param sourceFilePath
	 * @return source file raw checksum string
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private String getSourceFileChecksum(String sourceFilePath) throws IOException, URISyntaxException {

		Configuration conf = new Configuration();
		MD5MD5CRC32FileChecksum md5Checksum = null;
		int bytesPerCRC = DataValidationConstants.CHECKSUM_BYTE_PER_CRC;
		int blockSize = DataValidationConstants.CHECKSUM_BLOCK_SIZE;
		Path srcPath = new Path(sourceFilePath);
		FileSystem srcFs = LocalFileSystem.get(new URI(sourceFilePath), conf);
		long fileSize = 0;
		int blockCount = 0;
		DataOutputBuffer md5outDataBuffer = new DataOutputBuffer();
		DataChecksum checksum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C,
				DataValidationConstants.CHECKSUM_BYTE_PER_CRC);
		InputStream inputStream = null;
		long crc_per_block = blockSize / bytesPerCRC;
		fileSize = srcFs.getFileStatus(srcPath).getLen();
		blockCount = (int) Math.ceil((double) fileSize / (double) blockSize);

		inputStream = srcFs.open(srcPath);
		long totalBytesRead = 0;

		for (int x = 0; x < blockCount; x++) {
			ByteArrayOutputStream ar_CRC_Bytes = new ByteArrayOutputStream();
			byte crc[] = new byte[DataValidationConstants.BUFFER_SIZE_4];
			byte buf[] = new byte[DataValidationConstants.BUFFER_SIZE_512];

			int bytesRead = 0;
			while ((bytesRead = inputStream.read(buf)) > 0) {
				totalBytesRead += bytesRead;
				checksum.reset();
				checksum.update(buf, 0, bytesRead);
				checksum.writeValue(crc, 0, true);
				ar_CRC_Bytes.write(crc);
				if (totalBytesRead >= (long) (x + 1) * blockSize) {
					break;
				}
			}
			DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(ar_CRC_Bytes.toByteArray()));

			final MD5Hash md5_dataxceiver = MD5Hash.digest(dataInputStream);
			md5_dataxceiver.write(md5outDataBuffer);

			ar_CRC_Bytes.close();
			dataInputStream.close();

		}
		MD5Hash md5_of_md5 = MD5Hash.digest(md5outDataBuffer.getData());
		md5Checksum = new MD5MD5CRC32FileChecksum(bytesPerCRC, crc_per_block, md5_of_md5);

		inputStream.close();
		srcFs.close();
		String localFileChecksum = md5Checksum.toString().split(DataConstants.COLON)[1];

		return localFileChecksum;
	}

	/**
	 * This is to check errorChecksumDir exists or not in hdfs
	 * 
	 * @param filePath
	 * @return true if errorChecksumDir exists else return false
	 * @throws IOException
	 */
	private boolean checkErrorChecksumDirExists(String filePath) throws IOException {
		HttpResponse response = webHdfs.fileStatus(filePath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 404) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * This is to make error checksum directory in hdfs
	 * 
	 * @param dirPath
	 * @return true if create directory successfully else return false
	 * @throws IOException
	 */
	private boolean makeErrorChecksumDir(String dirPath) throws IOException {
		HttpResponse response = webHdfs.mkdir(dirPath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 200) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method is move errorChecksumFile to another location in hdfs
	 * 
	 * @param source
	 *            errorChecksumFile path
	 * @param dest
	 *            error checksum directory
	 * @throws IOException
	 */
	private void moveErrorChecksumFile(String source, String dest) throws IOException {
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
