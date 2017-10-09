/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

public class WebHdfsTest {
  HttpResponse httpResponse;

  /**
   * Make sure constructor is setting appropriate parameters.
   */
  @Test
  public void testConstructor() {
    WebHdfs webHdfs = new WebHdfs("host", 0);
    Assert.assertNotNull(webHdfs.getJson());
    Assert.assertEquals(webHdfs.getHost(), "host");
  }

  /**
   * Make sure getInstance works exactly like constructor and is setting
   * appropriate parameters.
   */
  @Test
  public void testGetInstance() {
    WebHdfs webHdfs = WebHdfs.getInstance("host1", 0);
    Assert.assertNotNull(webHdfs.getJson());
    Assert.assertEquals(webHdfs.getHost(), "host1");
  }

  /**
   * Set parameter should set the fields from input ObjectNode.
   *
   * @throws JsonProcessingException
   * @throws IOException
   */
  @Test
  public void testSetParameters() throws JsonProcessingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode actualObj = (ObjectNode) (mapper.readTree("{\"k1\": \"v1\", \"k\" : \"null\"}"));
    WebHdfs webHdfs = new WebHdfs("host", 0);
    webHdfs.setParameters(actualObj);
    Assert.assertEquals(webHdfs.getJson().get("k1").asText(), "v1");
  }

  /**
   * Make sure that headers passed in argument is actually set on the WebHdfs
   * object
   */
  @Test
  public void testAddHeaders() {
    WebHdfs webHdfs = new WebHdfs("host", 0);
    List<Header> headers = Mockito.mock(List.class);
    webHdfs.addHeaders(headers);
    List<Header> actualHeaders = (List<Header>) ReflectionTestUtils.getField(webHdfs, "headers");
    Assert.assertSame(actualHeaders, headers);
  }

  /**
   * Make sure that headers passed in argument is actually added to
   * Webhdfs.headers object
   */
  @Test
  public void testAddHeader() {
    WebHdfs webHdfs = new WebHdfs("host", 0);
    webHdfs.addHeader("h1", "v1");
    List<Header> actualHeaders = (List<Header>) ReflectionTestUtils.getField(webHdfs, "headers");
    Assert.assertEquals(actualHeaders.get(0).getName(), "h1");
    Assert.assertEquals(actualHeaders.get(0).getValue(), "v1");
  }

  /**
   * Add parameter should add fields to local json.
   *
   * @throws JsonProcessingException
   * @throws IOException
   */
  @Test
  public void testAddParameter() throws JsonProcessingException, IOException {
    WebHdfs webHdfs = new WebHdfs("host", 0);
    webHdfs.addParameter("unit-key", "unit-value");
    Assert.assertEquals(webHdfs.getJson().get("unit-key").asText(), "unit-value");
  }

  /**
   * buildURI method should get all the parameters from internal json node and
   * add to uri.
   */
  @Test
  public void testBuildUriWithJson() {
    WebHdfs webHdfs = new WebHdfs("host", 0);

    ObjectNode jsonParameters = Mockito.mock(ObjectNode.class);
    @SuppressWarnings("unchecked")
    Iterator<String> keys = Mockito.mock(Iterator.class);
    Mockito.when(jsonParameters.fieldNames()).thenReturn(keys);

    JsonNode jsonNode = Mockito.mock(JsonNode.class);
    Mockito.when(jsonParameters.get(Mockito.anyString())).thenReturn(jsonNode);
    Mockito.when(jsonNode.asText()).thenReturn("v1");
    ReflectionTestUtils.setField(webHdfs, "jsonParameters", jsonParameters);

    Mockito.when(keys.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    Mockito.when(keys.next()).thenReturn("k1").thenReturn("k2");
    webHdfs.buildURI("op", "path");
    Mockito.verify(jsonParameters, Mockito.times(1)).get("k1");
    Mockito.verify(jsonParameters, Mockito.times(1)).get("k2");
    Mockito.verify(jsonNode, Mockito.times(2)).asText();
  }

  /**
   * Variant of testBuildUriWithJson, with an edge case where the value of a
   * parameter is "null".
   */
  @Test
  public void testBuildUriWithJsonAndNullText() {
    WebHdfs webHdfs = new WebHdfs("host", 0);
    webHdfs.addParameter("k1", "null");
    webHdfs.buildURI("op", "path");
  }

  private HttpResponse setupJsonResponse() throws UnsupportedOperationException, IOException {
    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
    InputStream is = new ByteArrayInputStream("{\"k\" : \"v\"}".getBytes());

    Header header = Mockito.mock(Header.class);
    Header[] headers = new Header[1];
    headers[0] = header;
    Mockito.when(mockHttpResponse.getAllHeaders()).thenReturn(headers);
    Mockito.when(header.getName()).thenReturn("Content-Type");
    Mockito.when(header.getValue()).thenReturn("application/json");

    Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    Mockito.when(mockEntity.getContent()).thenReturn(is);
    return mockHttpResponse;
  }

  /**
   * releaseConnection method on WebHdfs should internally invoke
   * releaseConnection on HttpRequestBase.
   */
  @Test
  public void testReleaseConnection() {
    HttpRequestBase httpRequestBase = Mockito.mock(HttpRequestBase.class);
    WebHdfs webHdfs = new WebHdfs("host", 0);
    ReflectionTestUtils.setField(webHdfs, "httpRequest", httpRequestBase);
    webHdfs.releaseConnection();
    Mockito.verify(httpRequestBase, Mockito.times(1)).releaseConnection();

  }

  /**
   * openConnection setHosts if the host is not null.
   */
//    @Test
//    public void testOpenConnection() {
//        WebHdfs webHdfs = new WebHdfs("host", 0);
//        RoundRobinStrategy roundRobinStrategy = Mockito.mock(RoundRobinStrategy.class);
//        roundRobinStrategy.hostList = null;
//        ReflectionTestUtils.setField(webHdfs, "roundRobinStrategy", roundRobinStrategy);
//        webHdfs.openConnection();
//        Mockito.verify(roundRobinStrategy, Mockito.times(1)).setHosts(Mockito.anyString());
//    }

  /**
   * openConnection does NOT setHosts if the host is null.
   */
//    @Test
//    public void testOpenConnectionWithNullHost() {
//        WebHdfs webHdfs = new WebHdfs("host", 0);
//        RoundRobinStrategy roundRobinStrategy = Mockito.mock(RoundRobinStrategy.class);
//        roundRobinStrategy.hostList = null;
//        ReflectionTestUtils.setField(webHdfs, "roundRobinStrategy", roundRobinStrategy);
//        ReflectionTestUtils.setField(webHdfs, "host", null);
//        webHdfs.openConnection();
//        Mockito.verify(roundRobinStrategy, Mockito.times(0)).setHosts(Mockito.anyString());
//    }
  @Test
  public void testCreateAndWrite() throws ClientProtocolException, IOException {
    setupInstance().createAndWrite("hdfspath", "filepath");
  }

  @Test
  public void testCreateAndWriteWithInputStream() throws ClientProtocolException, IOException {
    setupInstance().createAndWrite("hdfspath", Mockito.mock(FileInputStream.class));
  }

  /**
   * releaseConnection method on WebHdfs should internally invoke
   * releaseConnection on HttpRequestBase.
   *
   * @throws IOException
   * @throws ClientProtocolException
   */
  @Test
  public void testAppend() throws ClientProtocolException, IOException {
    setupInstance().append("hdfspath", "filepath");
  }

  /**
   * @throws ClientProtocolException
   * @throws IOException
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendWithNullLocation() throws ClientProtocolException, IOException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    WebHdfs webHdfs = new WebHdfs("host", 0);
    ReflectionTestUtils.setField(webHdfs, "httpClient", httpClient);

    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any(HttpRequestBase.class))).thenReturn(httpResponse);
    Header header = Mockito.mock(Header.class);
    Header[] headers = new Header[1];
    headers[0] = header;
    Mockito.when(httpResponse.getAllHeaders()).thenReturn(headers);
    Mockito.when(header.getName()).thenReturn("");
    webHdfs.append("hdfspath", "filepath");
  }

  @Test
  public void testAppendWithInputStream() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().append("hdfspath", Mockito.mock(FileInputStream.class));
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testOpenFile() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().openFile("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testMkdir() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().mkdir("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testRename() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().rename("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testDeleteFile() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().deleteFile("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testFileStatus() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().fileStatus("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testListStatus() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().listStatus("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testChecksum() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().checksum("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  @Test
  public void testSetPermission() throws ClientProtocolException, IOException {
    HttpResponse actualResponse = setupInstance().setPermission("hdfspath");
    Assert.assertNotNull(actualResponse);
    Assert.assertSame(actualResponse, httpResponse);
  }

  private WebHdfs setupInstance() throws ClientProtocolException, IOException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    WebHdfs webHdfs = new WebHdfs("host", 0);
    ReflectionTestUtils.setField(webHdfs, "httpClient", httpClient);

    httpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(httpClient.execute(Mockito.any(HttpRequestBase.class))).thenReturn(httpResponse);
    Mockito.when(httpClient.execute(Mockito.any(HttpRequestBase.class), Mockito.any(HttpClientContext.class))).thenReturn(httpResponse);
    Header header = Mockito.mock(Header.class);
    Header[] headers = new Header[1];
    headers[0] = header;
    Mockito.when(httpResponse.getAllHeaders()).thenReturn(headers);
    Mockito.when(header.getName()).thenReturn("Location");
    Mockito.when(header.getValue()).thenReturn("http://localhost");
    return webHdfs;
  }

}
