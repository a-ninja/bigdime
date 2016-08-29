/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation.integration;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.validation.HdfsRecordCountValidator;

public class HdfsRecordCountValidatorIntegrationTest{
	
	private static final Logger logger = LoggerFactory.getLogger(HdfsRecordCountValidatorIntegrationTest.class);
	private String remoteFile1 = "/webhdfs/v1/test1/20120218/0900/test-20120218-0900.txt";
	private String remoteFile2 = "/webhdfs/v1/test1/test-no-partition.txt";
	
    @BeforeTest
	public void setup() {
		logger.info("Setting the environment");
	}
	
    @Test(priority = 1)
    public void testWriteToHdfs() throws ClientProtocolException, IOException{
    	Assert.assertEquals(true, writeFile("test-20120218-0900.txt", remoteFile1));
    	Assert.assertEquals(true, writeFile("test-no-partition.txt", remoteFile2));
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHostName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "");
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullPort() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "");
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullUserName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, null);
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = NumberFormatException.class)
    public void testParsePortStringToIntException() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "Hello");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "test");
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullSrcRecordCount() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, null);
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = NumberFormatException.class)
    public void testParseSrcRecordCountStringToInt() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "count");
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHdfsPath() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "");
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHdfsFileName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "");
    	hdfsRecordCountValidator.validate(actionEvent);
    }
    
    @Test(priority = 2)
    public void testValidateRecordCountWithPartitionsDiff() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "234");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "test-20120218-0900.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "20120218, 0900");
    	Assert.assertEquals(hdfsRecordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
    
    @Test(priority = 3)
    public void testValidateRecordCountWithoutPartitions() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "1");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "test-no-partition.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
    	Assert.assertEquals(hdfsRecordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
    }

    @Test(priority = 4)
    public void testValidateRecordCountWithRCErrorDirExists() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HOST_NAMES, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PORT, "50070");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.USER_NAME, "root");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/test1/");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_FILE_NAME, "test-no-partition.txt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
    	Assert.assertEquals(hdfsRecordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
	
	@Test
	public void testSettersAndGetters(){
		HdfsRecordCountValidator hdfsRecordCountValidator= new HdfsRecordCountValidator();
		hdfsRecordCountValidator.setName("testName");
		Assert.assertEquals(hdfsRecordCountValidator.getName(), "testName");
	}
	
	private boolean writeFile(String sourceFileName, String remoteFilePath) throws ClientProtocolException, 
			IOException{
		boolean successful = false;
		WebHdfs webHdfs = WebHdfs.getInstance("sandbox.hortonworks.com", 50070)
							.addParameter(ActionEventHeaderConstants.USER_NAME, "root");
		HttpResponse response = webHdfs.fileStatus(remoteFilePath);
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(sourceFileName);
		if(response.getStatusLine().getStatusCode() == 404){
			HttpResponse response1 = webHdfs.createAndWrite(remoteFilePath, inputStream);
			if(response1.getStatusLine().getStatusCode()==201){
				logger.info("Successfully write into HDFS");
				successful = true;
			}else{
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
