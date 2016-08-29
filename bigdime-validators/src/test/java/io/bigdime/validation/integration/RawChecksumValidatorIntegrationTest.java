/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation.integration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.validation.RawChecksumValidator;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RawChecksumValidatorIntegrationTest {
	
	private static final Logger logger = LoggerFactory.getLogger(RawChecksumValidatorIntegrationTest.class);
	private String remoteFile1 = "/webhdfs/v1/test1/20120218/0900/checksum-20120218-0900.txt";
	private String remoteFile2 = "/webhdfs/v1/test1/checksum-no-partition.txt";
	private String source_no_partition = (new File("src/test/resources/checksum-no-partition.txt")).getAbsolutePath();
	private String source_partition = (new File("src/test/resources/checksum-20120218-0900.txt")).getAbsolutePath();
	private String source_error_checksum = (new File("src/test/resources/test-error-checksum.txt")).getAbsolutePath(); 
	
	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
	}
	
	@Test(priority = 1)
	public void writeFilesToHdfs() throws ClientProtocolException, IOException{
		Assert.assertEquals(true, writeFile("checksum-20120218-0900.txt", remoteFile1));
    	Assert.assertEquals(true, writeFile("checksum-no-partition.txt", remoteFile2));
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullHostName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator = new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "");
    	rawChecksumValidator.validate(actionEvent);
    }
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullPortName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "");
    	rawChecksumValidator.validate(actionEvent);
    }
	
	@Test
	public void testNotReadyToValidate() throws DataValidationException{
		ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "false");
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.NOT_READY);
	}
	
	@Test(expectedExceptions = NumberFormatException.class)
	public void testConvertPortStringToInt() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "hello");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	rawChecksumValidator.validate(actionEvent);
    }
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullHdfsPath() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "2345");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, null);
    	rawChecksumValidator.validate(actionEvent);
    }
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullHdfsFileName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "2345");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "");
    	rawChecksumValidator.validate(actionEvent);
    }
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullSourcePath() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, "");
    	rawChecksumValidator.validate(actionEvent);
    }
	
	@Test
	public void testLocalFileNotFound() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "test_host");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "2345");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, "Testing");
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
	
	@Test
	public void testHdfsFileNotFound() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "testString");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, source_no_partition);
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
	
	@Test(priority = 2)
	public void testChecksumWithPartition() throws DataValidationException{
		ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "checksum-20120218-0900.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, source_partition);
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "20120218, 0900");
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
	}
	
	@Test(priority = 3)
	public void testChecksumWithoutPartition() throws DataValidationException{
		ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "checksum-no-partition.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, source_no_partition);
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
	}
	
	@Test(priority = 4)
	public void testChecksumFailedWithoutPartition() throws DataValidationException{
		ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "checksum-no-partition.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, source_error_checksum);
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
	}
	
	@Test(priority = 5)
	public void testChecksumFailedWithErrorChecksumDirExists() throws DataValidationException{
		ActionEvent actionEvent = new ActionEvent();
    	RawChecksumValidator rawChecksumValidator= new RawChecksumValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "checksum-20120218-0900.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, source_error_checksum);
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "20120218, 0900");
    	Assert.assertEquals(rawChecksumValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
	}
	
	@Test
	public void testGettersAndSetters() throws DataValidationException {
		RawChecksumValidator rawChecksumValidator = new RawChecksumValidator();
		rawChecksumValidator.setName("unit-name");
		Assert.assertEquals(rawChecksumValidator.getName(), "unit-name");

	}
	
	private boolean writeFile(String sourceFileName, String remoteFilePath)
			throws ClientProtocolException, IOException {
		boolean successful = false;
		WebHdfs webHdfs = WebHdfs.getInstance("sandbox.hortonworks.com", 50070)
							.addParameter(ActionEventHeaderConstants.USER_NAME, "hdfs");
		HttpResponse response = webHdfs.fileStatus(remoteFilePath);
		InputStream inputStream = getClass().getClassLoader()
				.getResourceAsStream(sourceFileName);
		if (response.getStatusLine().getStatusCode() == 404) {
			HttpResponse response1 = webHdfs.createAndWrite(remoteFilePath,
					inputStream);
			if (response1.getStatusLine().getStatusCode() == 201) {
				logger.info("Successfully write into HDFS");
				successful = true;
			} else {
				logger.error("Failed to write to HDFS.");
				successful = false;
			}
		}
		webHdfs.releaseConnection();
		inputStream.close();
		return successful;
	}
	
	@AfterTest
	public void cleanUp() throws ClientProtocolException, IOException{
		WebHdfs webHdfs = WebHdfs.getInstance("sandbox.hortonworks.com", 50070);
		webHdfs.deleteFile(remoteFile1);
		webHdfs.deleteFile(remoteFile2);
		webHdfs.releaseConnection();
		logger.info("**********Finish***********");
	}
}
