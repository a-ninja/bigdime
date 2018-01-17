/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class WebHdfsReaderTest {
	WebHdfs webHdfs;

	@Test(enabled = false)
	public void testGetFileType() throws InterruptedException, IOException, WebHdfsException {

		gen(200, 200, 200);
		WebHdfsReader webHdfsReader = new WebHdfsReader("", 0, "", HDFS_AUTH_OPTION.KERBEROS);
		webHdfsReader.fileType("/");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).releaseConnection();
	}

	@Test(enabled = false)
	public void testList() throws IOException, InterruptedException, WebHdfsException {

		gen(201, 200, 200);
		WebHdfsReader webHdfsReader = new WebHdfsReader("", 0, "", HDFS_AUTH_OPTION.KERBEROS);
		webHdfsReader.list("/", false);

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).listStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).releaseConnection();
	}

	private void gen(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode, boolean throwException)
			throws ClientProtocolException, IOException, InterruptedException {
		webHdfs = Mockito.mock(WebHdfs.class);

		HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine = Mockito.mock(StatusLine.class);
		Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(fileStatusStatusCode);
		HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(httpEntity.getContent())
				.thenReturn(new ByteArrayInputStream(
						"{\"FileStatus\":{\"accessTime\":1398090274505,\"blockSize\":34217472,\"childrenNum\":0,\"fileId\":16961,\"group\":\"hdfs\",\"length\":1878,\"modificationTime\":1398090274993,\"owner\":\"ambari-qa\",\"pathSuffix\":\"\",\"permission\":\"644\",\"replication\":1,\"type\":\"DIRECTORY\"}}"
								.getBytes()));
		Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

		HttpResponse listStatusResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine1 = Mockito.mock(StatusLine.class);
		Mockito.when(listStatusResponse.getStatusLine()).thenReturn(statusLine1);
		Mockito.when(statusLine1.getStatusCode()).thenReturn(fileStatusStatusCode);
		HttpEntity listStatusEntity = Mockito.mock(HttpEntity.class);

		Mockito.when(listStatusEntity.getContent())
				.thenReturn(new ByteArrayInputStream(
						"{\"FileStatuses\":{\"FileStatus\":[{\"accessTime\":1398090274505,\"blockSize\":34217472,\"childrenNum\":0,\"fileId\":16961,\"group\":\"hdfs\",\"length\":1878,\"modificationTime\":1398090274993,\"owner\":\"ambari-qa\",\"pathSuffix\":\"\",\"permission\":\"644\",\"replication\":1,\"type\":\"FILE\"}]}}"
								.getBytes()));
		Mockito.when(listStatusResponse.getEntity()).thenReturn(listStatusEntity);

		Mockito.when(webHdfs.fileStatus(Mockito.anyString())).thenReturn(httpResponse);

		Mockito.when(webHdfs.listStatus(Mockito.anyString())).thenReturn(listStatusResponse);
	}

	private void gen(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode)
			throws ClientProtocolException, IOException, InterruptedException {
		gen(mkdirStatusCode, fileStatusStatusCode, appendCreateStatusCode, false);
	}

}
