package io.bigdime.handler.hive;

import io.bigdime.libs.hdfs.FileStatus;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsReader;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

@ContextConfiguration(classes = HiveJobOutputFileValidatorTestConfig.class)
public class HiveJobOutputFileValidatorTest {
  @InjectMocks
  HiveJobOutputFileValidator fileValidator;

  @Mock
  WebHdfsReader webHdfsReader;

  @BeforeMethod
  public void init() {
    System.out.println("init");
    MockitoAnnotations.initMocks(this);
    System.out.println("fileValidator=" + fileValidator);
    System.out.println("webHdfsReader=" + webHdfsReader);
    // ReflectionTestUtils.setField(fileValidator, "webHdfsReader",
    // webHdfsReader);
  }

  @Test
  public void testValidateOutputFile() throws IOException, WebHdfsException {
    // WebHdfsReader webHdfsReader = Mockito.mock(WebHdfsReader.class);
    Mockito.when(webHdfsReader.getFileStatus(Mockito.anyString())).thenReturn(Mockito.mock(FileStatus.class));
    // HiveJobOutputFileValidator fileValidator = new
    // HiveJobOutputFileValidator(webHdfsReader);
    boolean validated = fileValidator.validateOutputFile("");
    Assert.assertTrue(validated);
  }

  @Test(expectedExceptions = Exception.class)
  public void testValidateOutputFileWithException() throws IOException, WebHdfsException {
    Exception ex = new Exception("Not Found");
    IOException ioEx = new IOException(ex);

    Mockito.when(webHdfsReader.getFileStatus(Mockito.anyString())).thenThrow(ioEx);
    try {
      fileValidator.validateOutputFile("");
    } catch (Exception ex1) {
      Mockito.verify(webHdfsReader, Mockito.times(1)).getFileStatus(Mockito.anyString());
      throw ex1;
    }
  }

  @Test
  public void testValidateOutputFileNotFound() throws IOException, WebHdfsException {
    WebHdfsException ex = new WebHdfsException(404, "Not Found");

    Mockito.when(webHdfsReader.getFileStatus(Mockito.anyString())).thenThrow(ex);
    boolean validated = fileValidator.validateOutputFile("");
    Assert.assertFalse(validated);
    Mockito.verify(webHdfsReader, Mockito.times(1)).getFileStatus(Mockito.anyString());
  }

  @Test(expectedExceptions = WebHdfsException.class)
  public void testValidateOutputFileException() throws IOException, WebHdfsException {
    // WebHdfsReader webHdfsReader = Mockito.mock(WebHdfsReader.class);
    Exception ex = new Exception("unit test");
    WebHdfsException webEx = new WebHdfsException("unit test", ex);

    Mockito.when(webHdfsReader.getFileStatus(Mockito.anyString())).thenThrow(webEx);
    // HiveJobOutputFileValidator fileValidator = new
    // HiveJobOutputFileValidator(webHdfsReader);
    boolean validated = fileValidator.validateOutputFile("");
    Assert.assertFalse(validated);
  }
}
