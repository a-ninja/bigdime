/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hdfs.WebHdfs;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RawChecksumUnzipValidatorTest {
	
	private static final Logger logger = LoggerFactory.getLogger(RawChecksumUnzipValidatorTest.class);
	
	@Test
	public void validationNotReadyTest() throws DataValidationException{
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.FALSE.toString());
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.NOT_READY);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void nullHdfsHostTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void nullHdfsPortTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = NumberFormatException.class)
	public void parsePortStringToIntTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "port");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void nullHdfsPathTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void nullHdfsFileNameTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void nullSourceFilePathTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void nullSourceFileNameTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, "sourcePath");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		unzipRawChecksumValidator.validate(mockActionEvent);
	}
	
	@Test
	public void sourceFileNotFoundTest() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, "sourcePath");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "sourceFileName");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockFile = Mockito.mock(File.class);
		when(mockFile.exists()).thenReturn(false);
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void noContentInSourceZipFileTest() throws DataValidationException, IOException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		File f = new File("src/test/resources/test.zip");
    	ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
		out.closeEntry();
    	out.close();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, f.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "test.txt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "/");
		when(mockTmpDir.exists()).thenReturn(false);
		when(mockTmpDir.mkdir()).thenReturn(true);
		unzipRawChecksumValidator.validate(mockActionEvent);
		if(f.delete()){
			logger.info("Successfully deleted zip file{}", f.getName());
		}else{
			logger.error("Unable to delete zip file{}", f.getName());
		}
		
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void fileNotFoundInSourceZipFileTest() throws DataValidationException, IOException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		String writeData = "fileNotFoundInSourceZipFileTest";
		File f = new File("src/test/resources/test.zip");
	    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
	    ZipEntry e = new ZipEntry("/stg/test.txt");
	    out.putNextEntry(e);
	    byte[] data = writeData.getBytes(Charset.forName("UTF-8"));
	    out.write(data, 0, data.length);    
	    out.closeEntry();
	    out.close();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, f.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "sourceFileName.txt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "/");
		when(mockTmpDir.exists()).thenReturn(true);
		unzipRawChecksumValidator.validate(mockActionEvent);
		if(f.delete()){
			logger.info("Successfully deleted zip file{}", f.getName());
		}else{
			logger.error("Unable to delete zip file{}", f.getName());
		}
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void failedToGetHdfsChecksumWithIOExceptionTest() throws DataValidationException, IOException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		String writeData ="failedToGetHdfsChecksumWithIOExceptionTest";
		File zipFile = createZipFile(writeData);
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, zipFile.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "test.txt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "src/test/resources/");
		Mockito.when(mockTmpDir.exists()).thenReturn(true);
		File mockLocalDirFile = Mockito.mock(File.class);
		Mockito.when(mockLocalDirFile.exists()).thenReturn(false);
		Mockito.when(mockLocalDirFile.mkdir()).thenReturn(true);
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "webHdfs", mockWebHdfs);
		String contents = "";
		getMockHDFSChecksum(mockWebHdfs, contents);
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
		if(zipFile.delete()){
			logger.info("Successfully deleted zip file{}", zipFile.getName());
		}else{
			logger.error("Unable to delete zip file{}", zipFile.getName());
		}
	}
	
	@Test
	public void unzipRawChecksumValidationPassedWithNoPartitionTest() throws DataValidationException, IOException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		String writeData ="unzipRawChecksumValidationPassedWithNoPartitionTest";
		File zipFile = createZipFile(writeData);
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, zipFile.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "test.txt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "src/test/resources/");
		Mockito.when(mockTmpDir.exists()).thenReturn(true);
		File mockLocalDirFile = Mockito.mock(File.class);
		Mockito.when(mockLocalDirFile.exists()).thenReturn(false);
		Mockito.when(mockLocalDirFile.mkdir()).thenReturn(true);
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "webHdfs", mockWebHdfs);
		String contents = "{\"FileChecksum\":{\"algorithm\":\"test_algorithm\",\"bytes\":\"000002000000000000000000827f1b5cbfadc7f6a9522d0d628a020e00000000\",\"length\":28}}";
		getMockHDFSChecksum(mockWebHdfs, contents);
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.PASSED);
		if(zipFile.delete()){
			logger.info("Successfully deleted zip file{}", zipFile.getName());
		}else{
			logger.error("Unable to delete zip file{}", zipFile.getName());
		}
	}
	
	@Test
	public void unzipRawChecksumValidationFailedWithPartitionTest() throws IOException, DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		String writeData ="unzipRawChecksumValidationFailedWithPartitionTest";
		File zipFile = createZipFile(writeData);
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, zipFile.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "test.txt");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "account, dt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "src/test/resources/");
		Mockito.when(mockTmpDir.exists()).thenReturn(true);
		File mockLocalDirFile = Mockito.mock(File.class);
		Mockito.when(mockLocalDirFile.exists()).thenReturn(true);
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "webHdfs", mockWebHdfs);
		String contents = "{\"FileChecksum\":{\"algorithm\":\"test_algorithm\",\"bytes\":\"0000020000000000000000007479856d72b6bc6d4ab7442af836a3a600000000\",\"length\":28}}";
		getMockHDFSChecksum(mockWebHdfs, contents);
		mockCreateChecksumErrorDir(mockWebHdfs);
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
		if(zipFile.delete()){
			logger.info("Successfully deleted zip file{}", zipFile.getName());
		}else{
			logger.error("Unable to delete zip file{}", zipFile.getName());
		}
	}
	
	@Test
	public void unzipRawChecksumValidationFailedWithErrDirExistTest() throws IOException, DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		String writeData ="unzipRawChecksumValidationFailedWithErrDirExistTest";
		File zipFile = createZipFile(writeData);
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, zipFile.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "test.txt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "src/test/resources");
		Mockito.when(mockTmpDir.exists()).thenReturn(true);
		File mockLocalDirFile = Mockito.mock(File.class);
		Mockito.when(mockLocalDirFile.exists()).thenReturn(true);
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "webHdfs", mockWebHdfs);
		String contents = "{\"FileChecksum\":{\"algorithm\":\"test_algorithm\",\"bytes\":\"0000020000000000000000007479856d72b6bc6d4ab7442af836a3a600000000\",\"length\":28}}";
		getMockHDFSChecksum(mockWebHdfs, contents);
		mockDirExists(mockWebHdfs);
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
		if(zipFile.delete()){
			logger.info("Successfully deleted zip file{}", zipFile.getName());
		}else{
			logger.error("Unable to delete zip file{}", zipFile.getName());
		}
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void unableToMoveChecksumErrorTest() throws DataValidationException, IOException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		String writeData ="unableToMoveChecksumErrorTest";
		File zipFile = createZipFile(writeData);
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, "host");
		headers.put(ActionEventHeaderConstants.PORT, "123");
		headers.put(ActionEventHeaderConstants.HDFS_PATH, "/webhdfs/v1/hdfsPath");
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, "hdfsFile");
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_PATH, zipFile.getAbsolutePath());
		headers.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "test.txt");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		File mockTmpDir = Mockito.mock(File.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "extractCompressedFileLocation", "src/test/resources/");
		Mockito.when(mockTmpDir.exists()).thenReturn(true);
		File mockLocalDirFile = Mockito.mock(File.class);
		Mockito.when(mockLocalDirFile.exists()).thenReturn(true);
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(unzipRawChecksumValidator, "webHdfs", mockWebHdfs);
		String contents = "{\"FileChecksum\":{\"algorithm\":\"test_algorithm\",\"bytes\":\"0000020000000000000000007479856d72b6bc6d4ab7442af836a3a600000000\",\"length\":28}}";
		getMockHDFSChecksum(mockWebHdfs, contents);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse);
		StatusLine mockSL = Mockito.mock(StatusLine.class);
		when(mockResponse.getStatusLine()).thenReturn(mockSL);
		when(mockSL.getStatusCode()).thenReturn(404);
		doNothing().when(mockWebHdfs).releaseConnection();
		HttpResponse mockResponse1 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.mkdir(anyString())).thenReturn(mockResponse1);
		StatusLine mockSL1 = Mockito.mock(StatusLine.class);
		when(mockResponse1.getStatusLine()).thenReturn(mockSL1);
		when(mockSL1.getStatusCode()).thenReturn(404);
		doNothing().when(mockWebHdfs).releaseConnection();
		Assert.assertEquals(unzipRawChecksumValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
		if(zipFile.delete()){
			logger.info("Successfully deleted zip file{}", zipFile.getName());
		}else{
			logger.error("Unable to delete zip file{}", zipFile.getName());
		}

	}
	
	@Test
	public void testGettersAndSetters() throws DataValidationException {
		UnzipRawChecksumValidator unzipRawChecksumValidator = new UnzipRawChecksumValidator();
		unzipRawChecksumValidator.setName("unit-name");
		Assert.assertEquals(unzipRawChecksumValidator.getName(), "unit-name");

	}
	
	private File createZipFile(String writeData) throws IOException {
	    File f = new File("src/test/resources/test.zip");
	    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
	    ZipEntry e = new ZipEntry("test.txt");
	    out.putNextEntry(e);
	    byte[] data = writeData.getBytes(Charset.forName("UTF-8"));
	    out.write(data, 0, data.length);    
	    out.closeEntry();
	    out.close();
	    return f;
	}
	
	private void getMockHDFSChecksum(WebHdfs mockWebHdfs, String contents) throws ClientProtocolException, IOException {
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.checksum(anyString())).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream hdfsInputStream = new ByteArrayInputStream(contents.getBytes(Charset.forName("UTF-8")));
		when(mockHttpEntity.getContent()).thenReturn(hdfsInputStream);
		doNothing().when(mockWebHdfs).releaseConnection();
	}
	
	private void mockCreateChecksumErrorDir(WebHdfs mockWebHdfs) throws ClientProtocolException, IOException {
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse);
		StatusLine mockSL = Mockito.mock(StatusLine.class);
		when(mockResponse.getStatusLine()).thenReturn(mockSL);
		when(mockSL.getStatusCode()).thenReturn(404);
		doNothing().when(mockWebHdfs).releaseConnection();
		HttpResponse mockResponse1 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.mkdir(anyString())).thenReturn(mockResponse1);
		StatusLine mockSL1 = Mockito.mock(StatusLine.class);
		when(mockResponse1.getStatusLine()).thenReturn(mockSL1);
		when(mockSL1.getStatusCode()).thenReturn(200);
		doNothing().when(mockWebHdfs).releaseConnection();
	}
	
	private void mockDirExists(WebHdfs mockWebHdfs) throws ClientProtocolException, IOException {
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse);
		StatusLine mockSL = Mockito.mock(StatusLine.class);
		when(mockResponse.getStatusLine()).thenReturn(mockSL);
		when(mockSL.getStatusCode()).thenReturn(200);
		doNothing().when(mockWebHdfs).releaseConnection();
	}
}
