/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.HdfsRecordCountValidator;
import io.bigdime.libs.hdfs.WebHdfs;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.testng.Assert;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class HdfsRecordCountValidatorTest {

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHostTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("");
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullPortNumberTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn(null);
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullUserNameTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);

		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "111");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, null);

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);

		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = NumberFormatException.class)
	public void validatePortNumberFormatTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);

		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "Hello");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullSrcRecordCountTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "");

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = NumberFormatException.class)
	public void validateSrcRCParseToIntTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "src");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHdfsPathTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "2342");
		mockMap.put(ActionEventHeaderConstants.HDFS_PATH, "");

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHdfsFileNameTest() throws DataValidationException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "453");
		mockMap.put(ActionEventHeaderConstants.HDFS_PATH, "path");
		mockMap.put(ActionEventHeaderConstants.HDFS_FILE_NAME, null);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hdfsRecordCountValidator.validate(mockActionEvent);
	}

	@Test
	public void recordCountWithoutPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "2342");
		mockMap.put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/path/");
		mockMap.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "fileName");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "");

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(hdfsRecordCountValidator, "webHdfs", mockWebHdfs);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(mockWebHdfs.openFile(any(String.class))).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream inputStream = new ByteArrayInputStream(
				"testString \n new line in file".getBytes(Charset.forName("UTF-8")));
		Mockito.when(mockHttpEntity.getContent()).thenReturn(inputStream);
		doNothing().when(mockWebHdfs).releaseConnection();

		HttpResponse mockResponse1 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse1);
		StatusLine mockSL1 = Mockito.mock(StatusLine.class);
		when(mockResponse1.getStatusLine()).thenReturn(mockSL1);
		when(mockSL1.getStatusCode()).thenReturn(404);
		doNothing().when(mockWebHdfs).releaseConnection();

		HttpResponse mockResponse2 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.mkdir(anyString())).thenReturn(mockResponse2);
		StatusLine mockSL2 = Mockito.mock(StatusLine.class);
		when(mockResponse2.getStatusLine()).thenReturn(mockSL2);
		when(mockSL2.getStatusCode()).thenReturn(200);
		doNothing().when(mockWebHdfs).releaseConnection();
		Assert.assertEquals(hdfsRecordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test
	public void recordCountWithPartitionMatchTest()
			throws DataValidationException, ClientProtocolException, IOException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = new HashMap<>();
		String hivePartitions = "dt, hr";

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "2");
		mockMap.put(ActionEventHeaderConstants.HDFS_PATH, "path");
		mockMap.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "fileName");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, hivePartitions);

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);

		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(hdfsRecordCountValidator, "webHdfs", mockWebHdfs);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(mockWebHdfs.openFile(any(String.class))).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream inputStream = new ByteArrayInputStream(
				"testString \n new line in file".getBytes(Charset.forName("UTF-8")));
		Mockito.when(mockHttpEntity.getContent()).thenReturn(inputStream);
		Assert.assertEquals(hdfsRecordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.PASSED);
	}

	@Test
	public void recordCountWithRCErrorDirExistsDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		HdfsRecordCountValidator hdfsRecordCountValidator = new HdfsRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);

		Map<String, String> mockMap = new HashMap<>();

		mockMap.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		mockMap.put(ActionEventHeaderConstants.PORT, "123");
		mockMap.put(ActionEventHeaderConstants.USER_NAME, "user");
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "2342");
		mockMap.put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/path/");
		mockMap.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "fileName");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);

		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(hdfsRecordCountValidator, "webHdfs", mockWebHdfs);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(mockWebHdfs.openFile(any(String.class))).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream inputStream = new ByteArrayInputStream(
				"testString \n new line in file".getBytes(Charset.forName("UTF-8")));
		Mockito.when(mockHttpEntity.getContent()).thenReturn(inputStream);
		doNothing().when(mockWebHdfs).releaseConnection();

		HttpResponse mockResponse1 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse1);
		StatusLine mockSL1 = Mockito.mock(StatusLine.class);
		when(mockResponse1.getStatusLine()).thenReturn(mockSL1);
		when(mockSL1.getStatusCode()).thenReturn(200);
		doNothing().when(mockWebHdfs).releaseConnection();
		Assert.assertEquals(hdfsRecordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test
	public void gettersAndSettersTest() {
		HdfsRecordCountValidator recordCountFromWebhdfsValidator = new HdfsRecordCountValidator();
		recordCountFromWebhdfsValidator.setName("testName");
		Assert.assertEquals(recordCountFromWebhdfsValidator.getName(), "testName");
	}

}