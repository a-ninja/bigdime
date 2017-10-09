/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import org.apache.http.client.ClientProtocolException;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

import static org.mockito.Matchers.*;

public class WebHdfsWriterTest {
  //	WebHdfs webHdfs;
//  @InjectMocks
//  WebHdfsWriter webHdfsWriter;
//  @Mock
//  WebhdfsFacade webHdfsFacade;

//	@Mock
//	WebHdfsWriter webHdfsWriter;

  @BeforeMethod(alwaysRun = true)
  public void setup() {
    MockitoAnnotations.initMocks(this); //This could be pulled up into a shared base class
//		System.out.println("webHdfsWriter="+webHdfsWriter);
  }

  /**
   * TODO: Refactor to make it work with WebhdfsFacade
   * <p>
   * Test that a directory can be created. And, if the file exists, append method is called to append the contents.
   *
   * @throws InterruptedException
   * @throws ClientProtocolException
   * @throws IOException
   */
  @Test
  public void testWriteAppendMkdir200() throws InterruptedException, ClientProtocolException, IOException, WebHdfsException, NoSuchMethodException {

    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    WebhdfsFacade webHdfsFacade = Mockito.mock(WebhdfsFacade.class);
    ReflectionTestUtils.setField(webHdfsWriter, "maxAttempts", 2);
    ReflectionTestUtils.setField(webHdfsWriter, "webHdfsFacade", webHdfsFacade);
    Mockito.when(webHdfsFacade.invokeWithRetry(any(Method.class), anyInt(), Matchers.anyObject())).thenReturn(true);
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");
    Method mkdirsMethod = WebhdfsFacade.class.getMethod("mkdirs", String.class);
    Mockito.verify(webHdfsFacade, Mockito.times(1)).invokeWithRetry(eq(mkdirsMethod), eq(2), any(WrappedArray.class));
    Method getFileStatusMethod = WebhdfsFacade.class.getMethod("getFileStatus", String.class);
    Mockito.verify(webHdfsFacade, Mockito.times(1)).invokeWithRetry(eq(getFileStatusMethod), eq(1), any(WrappedArray.class));
    Method appendMethod = WebhdfsFacade.class.getMethod("append", String.class, InputStream.class);
    Mockito.verify(webHdfsFacade, Mockito.times(1)).invokeWithRetry(eq(appendMethod), eq(2), any(WrappedArray.class));

    Mockito.verify(webHdfsFacade, Mockito.times(3)).invokeWithRetry(any(Method.class), anyInt(), any(WrappedArray.class));
  }

  /**
   * This test is not needed, since WebhdfsFacade.mkdirs return true for 200 0r 201
   *
   * @throws InterruptedException
   * @throws ClientProtocolException
   * @throws IOException
   */
  @Test(enabled = false)
  public void testWriteAppendMkdir201() throws InterruptedException, ClientProtocolException, IOException {

    gen(201, 200, 200);
    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

//		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  @Test
  public void testWriteCreate() throws InterruptedException, ClientProtocolException, IOException, WebHdfsException, NoSuchMethodException {

    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    WebhdfsFacade webHdfsFacade = Mockito.mock(WebhdfsFacade.class);
    ReflectionTestUtils.setField(webHdfsWriter, "maxAttempts", 2);
    ReflectionTestUtils.setField(webHdfsWriter, "webHdfsFacade", webHdfsFacade);
    WebHdfsException ex = new WebHdfsException(404, "");
    Mockito.when(webHdfsFacade.invokeWithRetry(any(Method.class), anyInt(), Matchers.anyObject())).thenReturn(true);//.thenThrow(ex);
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");
    Method mkdirsMethod = WebhdfsFacade.class.getMethod("mkdirs", String.class);
    Mockito.verify(webHdfsFacade, Mockito.times(1)).invokeWithRetry(eq(mkdirsMethod), eq(2), any(WrappedArray.class));
    Method getFileStatusMethod = WebhdfsFacade.class.getMethod("getFileStatus", String.class);
    Mockito.verify(webHdfsFacade, Mockito.times(1)).invokeWithRetry(eq(getFileStatusMethod), eq(1), any(WrappedArray.class));
//    Method appendMethod = WebhdfsFacade.class.getMethod("createAndWrite", String.class, InputStream.class);
//    Mockito.verify(webHdfsFacade, Mockito.times(1)).invokeWithRetry(eq(appendMethod), eq(2), any(WrappedArray.class));

    Mockito.verify(webHdfsFacade, Mockito.times(3)).invokeWithRetry(any(Method.class), anyInt(), any(WrappedArray.class));


//		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(1)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  /**
   * If the command to check file's existence returns anything other than
   * 200,201,404, then append or createAndWrite should not be invoked, since
   * it's an exception condition.
   * TODO: This test case should be covered by WebhdfsFacadeTest
   *
   * @throws InterruptedException
   * @throws ClientProtocolException
   * @throws IOException
   */
  @Test(expectedExceptions = IOException.class, enabled = false)
  public void testWriteCreateWithFileExistsReturning403()
          throws InterruptedException, ClientProtocolException, IOException {
    gen(200, 403, 200);
    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

//		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  @Test(enabled = false)
  public void testWriteCreateWith201() throws InterruptedException, ClientProtocolException, IOException {
    gen(200, 404, 201);
    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

//		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(1)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  @Test(expectedExceptions = IOException.class, enabled = false)
  public void testWriteCreateFailsWith404() throws InterruptedException, ClientProtocolException, IOException {
    gen(200, 404, 404);
    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    ReflectionTestUtils.setField(webHdfsWriter, "sleepTime", 1);
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

//		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(4)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  @Test(expectedExceptions = IOException.class, enabled = false)
  public void testWriteCreateFailsThrowsException()
          throws InterruptedException, ClientProtocolException, IOException {
    gen(200, 404, 0, true);
    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    ReflectionTestUtils.setField(webHdfsWriter, "sleepTime", 1);
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

//		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(4)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  @Test(expectedExceptions = IOException.class, enabled = false)
  public void testWriteMkdirFails() throws InterruptedException, ClientProtocolException, IOException {
    gen(404, 0, 0);
    WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
    ReflectionTestUtils.setField(webHdfsWriter, "sleepTime", 1);
    webHdfsWriter.write("unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

//		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).fileStatus(Mockito.anyString());
//		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
//		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
  }

  private void gen(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode, boolean throwException)
          throws ClientProtocolException, IOException, InterruptedException {
//		webHdfs = Mockito.mock(WebHdfs.class);

//		webHdfs = new WebHdfs("host", 0);

//    WebhdfsFacade webHdfsFacade = Mockito.mock(WebhdfsFacade.class);

//    HttpResponse mkdirHttpResponse = Mockito.mock(HttpResponse.class);
//		Mockito.when(webHdfs.mkdir(Mockito.anyString())).thenReturn(mkdirHttpResponse);
//    StatusLine mkdirStatusLine = Mockito.mock(StatusLine.class);
//    Mockito.when(mkdirHttpResponse.getStatusLine()).thenReturn(mkdirStatusLine);
//    Mockito.when(mkdirStatusLine.getStatusCode()).thenReturn(mkdirStatusCode);

//    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
//    StatusLine statusLine = Mockito.mock(StatusLine.class);
//    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
//    Mockito.when(statusLine.getStatusCode()).thenReturn(fileStatusStatusCode);

//    HttpResponse appendOrCreateHttpResponse = Mockito.mock(HttpResponse.class);
//    StatusLine statusLine1 = Mockito.mock(StatusLine.class);
//    Mockito.when(appendOrCreateHttpResponse.getStatusLine()).thenReturn(statusLine1);
//    Mockito.when(statusLine1.getStatusCode()).thenReturn(appendCreateStatusCode);

//		Mockito.when(webHdfs.fileStatus(Mockito.anyString())).thenReturn(httpResponse);
//		if (throwException)
//			Mockito.when(webHdfs.createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class)))
//					.thenThrow(Mockito.mock(IOException.class));
//		else
//			Mockito.when(webHdfs.createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class)))
//					.thenReturn(appendOrCreateHttpResponse);
//		Mockito.when(webHdfs.append(Mockito.anyString(), Mockito.any(InputStream.class)))
//				.thenReturn(appendOrCreateHttpResponse);
  }

  private void gen(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode)
          throws ClientProtocolException, IOException, InterruptedException {
    gen(mkdirStatusCode, fileStatusStatusCode, appendCreateStatusCode, false);
  }

}
