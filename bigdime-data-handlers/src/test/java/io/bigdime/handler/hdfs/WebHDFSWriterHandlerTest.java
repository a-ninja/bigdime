/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.SinkHandlerException;
import io.bigdime.core.commons.StringCase;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.constants.WebHDFSWriterHandlerConstants;
import io.bigdime.handler.webhdfs.WebHDFSWriterHandler;
import io.bigdime.handler.webhdfs.WebHDFSWriterHandlerJournal;
import io.bigdime.libs.hdfs.WebHdfs;

/**
 * 
 * @author Neeraj Jain
 *
 */
public class WebHDFSWriterHandlerTest {

	WebHdfs mockWebHdfs;
	RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	// @Test
	// public void testProcessEvent() throws HandlerException,
	// ClientProtocolException, IOException, InterruptedException,
	// AdaptorConfigurationException {
	// WebHDFSWriterHandler WebHDFSWriterHandler = mockWebHDFSWriterHandler();
	// ActionEvent actionEvent = mockActionEventWithHeaders();
	// ActionEvent actualEvent = WebHDFSWriterHandler.process();
	// Assert.assertEquals(actualEvent.getBody(), actionEvent.getBody());
	// Mockito.verify(mockWebHdfs,
	// Mockito.times(1)).append(Mockito.eq("/webhdfs/unitAccount/unitTimestamp/unitFile"),
	// Mockito.any(InputStream.class));
	// }

	/**
	 * If there are no events in journal and there are events in handler context
	 * and the events belong to different path, they need to be processed in
	 * different chains, so status=CALLBACK must be returned.
	 * 
	 * @throws HandlerException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testProcessWithEventsFromHandlerContextForCallback() throws HandlerException, ClientProtocolException,
			IOException, InterruptedException, AdaptorConfigurationException {
		WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
		ActionEvent actionEvent1 = mockActionEventWithHeaders();
		ActionEvent actionEvent2 = mockActionEventWithHeaders1();
		HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = new ArrayList<>();
		actionEvents.add(actionEvent1);
		actionEvents.add(actionEvent2);
		handlerContext.setEventList(actionEvents);
		Status status = webHDFSWriterHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
		Assert.assertNotNull(handlerContext.getJournal(webHDFSWriterHandler.getId()));
		Assert.assertTrue(((WebHDFSWriterHandlerJournal) handlerContext.getJournal(webHDFSWriterHandler.getId()))
				.getEventList().size() == 1);
	}

	/**
	 * If there are no events in journal and there are events in handler context
	 * and the events belong to same path, they need to be processed in same
	 * chains, so status=READY must be returned.
	 * 
	 * @throws HandlerException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testProcessWithEventsFromHandlerContextForReady() throws HandlerException, ClientProtocolException,
			IOException, InterruptedException, AdaptorConfigurationException {
		WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
		ActionEvent actionEvent1 = mockActionEventWithHeaders();
		ActionEvent actionEvent2 = mockActionEventWithHeaders();
		HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = new ArrayList<>();
		actionEvents.add(actionEvent1);
		actionEvents.add(actionEvent2);
		handlerContext.setEventList(actionEvents);
		Status status = webHDFSWriterHandler.process();
		Assert.assertSame(status, Status.READY);
		Assert.assertNull(handlerContext.getJournal(webHDFSWriterHandler.getId()));
	}

	/**
	 * If there are events in journal and the events belong to different path,
	 * they need to be processed in different chains, so status=CALLBACK must be
	 * returned.
	 * 
	 * @throws HandlerException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testProcessWithEventsFromJournalForCallback() throws HandlerException, ClientProtocolException,
			IOException, InterruptedException, AdaptorConfigurationException {
		WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
		ActionEvent actionEvent1 = mockActionEventWithHeaders();
		ActionEvent actionEvent2 = mockActionEventWithHeaders1();
		HandlerContext handlerContext = HandlerContext.get();

		WebHDFSWriterHandlerJournal journal = new WebHDFSWriterHandlerJournal();
		handlerContext.setJournal(webHDFSWriterHandler.getId(), journal);
		List<ActionEvent> actionEvents = new ArrayList<>();
		actionEvents.add(actionEvent1);
		actionEvents.add(actionEvent2);
		journal.setEventList(actionEvents);

		Status status = webHDFSWriterHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
		Assert.assertNotNull(handlerContext.getJournal(webHDFSWriterHandler.getId()));
		Assert.assertTrue(((WebHDFSWriterHandlerJournal) handlerContext.getJournal(webHDFSWriterHandler.getId()))
				.getEventList().size() == 1);
	}

	/**
	 * If there are events in journal and the events belong to same path, they
	 * need to be processed in same chains, so status=READY must be returned.
	 * 
	 * @throws HandlerException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testProcessWithEventsFromJournalForReady() throws HandlerException, ClientProtocolException,
			IOException, InterruptedException, AdaptorConfigurationException {
		WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
		ActionEvent actionEvent1 = mockActionEventWithHeaders();
		ActionEvent actionEvent2 = mockActionEventWithHeaders();
		HandlerContext handlerContext = HandlerContext.get();

		WebHDFSWriterHandlerJournal journal = new WebHDFSWriterHandlerJournal();
		handlerContext.setJournal(webHDFSWriterHandler.getId(), journal);
		List<ActionEvent> actionEvents = new ArrayList<>();
		actionEvents.add(actionEvent1);
		actionEvents.add(actionEvent2);
		journal.setEventList(actionEvents);

		Status status = webHDFSWriterHandler.process();
		Assert.assertSame(status, Status.READY);
		Assert.assertNull(handlerContext.getJournal(webHDFSWriterHandler.getId()));
	}

	@Test(expectedExceptions = HandlerException.class)
	public void testProcessWithNullEvent() throws HandlerException {
		WebHDFSWriterHandler hdfsWriterHandler = new WebHDFSWriterHandler();
		ActionEvent nullEvent = null;
		HandlerContext.get().createSingleItemEventList(nullEvent);
		hdfsWriterHandler.process();
	}

	@Test(expectedExceptions = HandlerException.class)
	public void testProcessWithException()
			throws HandlerException, ClientProtocolException, IOException, InterruptedException {

		WebHDFSWriterHandler WebHDFSWriterHandler = mockWebHDFSWriterHandler();
		ActionEvent actionEvent = mockActionEvent();
		HandlerContext.get().createSingleItemEventList(actionEvent);
		WebHDFSWriterHandler.process();
	}

	/**
	 * If there was any problem in writing to hdfs, SinkHandlerException should
	 * be thrown.
	 * 
	 * @throws SinkHandlerException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test(expectedExceptions = SinkHandlerException.class)
	public void testProcessWithWebHdfsException()
			throws HandlerException, ClientProtocolException, IOException, InterruptedException {

		WebHDFSWriterHandler WebHDFSWriterHandler = mockWebHDFSWriterHandler();
		ActionEvent actionEvent = mockActionEventWithHeaders();
		Mockito.doThrow(Mockito.mock(IOException.class)).when(mockWebHdfs).append(Mockito.anyString(),
				Mockito.any(InputStream.class));
		HandlerContext.get().createSingleItemEventList(actionEvent);
		WebHDFSWriterHandler.process();
	}

	@SuppressWarnings("unchecked")
	private WebHDFSWriterHandler mockWebHDFSWriterHandler()
			throws ClientProtocolException, IOException, InterruptedException {
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);

		Map<String, String> tokenToHeaderNameMap = new HashMap<>();
		tokenToHeaderNameMap.put("${account}", "ACCOUNT");
		tokenToHeaderNameMap.put("${timestamp}", "TIMESTAMP");
		mockWebHdfs = mockWebHdfs(200, 200, 200, false);
		Mockito.doNothing().when(mockWebHdfs).releaseConnection();
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "webHdfs", mockWebHdfs);
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "hdfsPath", "/webhdfs/${account}/${timestamp}");
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "hdfsFileName", "unitFile");
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "tokenToHeaderNameMap", tokenToHeaderNameMap);
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "runtimeInfoStore", runtimeInfoStore);
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "channelDesc", "unit-Channel");
		ReflectionTestUtils.setField(WebHDFSWriterHandler, "hdfsPathCaseEnum", StringCase.LOWER);

		return WebHDFSWriterHandler;

	}

	private ActionEvent mockActionEventWithHeaders() throws ClientProtocolException, IOException, InterruptedException {
		ActionEvent mockEvent = mockActionEvent();
		Map<String, String> headers = new HashMap<>();
		headers.put("ACCOUNT", "unitAccount");
		headers.put("TIMESTAMP", "unitTimestamp");
		Mockito.when(mockEvent.getHeaders()).thenReturn(headers);
		return mockEvent;
	}

	private ActionEvent mockActionEventWithHeaders1()
			throws ClientProtocolException, IOException, InterruptedException {
		ActionEvent mockEvent = mockActionEvent();
		Map<String, String> headers = new HashMap<>();
		headers.put("ACCOUNT", "unitAccount1");
		headers.put("TIMESTAMP", "unitTimestamp");
		Mockito.when(mockEvent.getHeaders()).thenReturn(headers);
		return mockEvent;
	}

	private ActionEvent mockActionEvent() throws ClientProtocolException, IOException, InterruptedException {
		ActionEvent actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn("unit-data".getBytes(Charset.defaultCharset()));
		return actionEvent;
	}

	private WebHdfs mockWebHdfs(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode,
			boolean throwException) throws ClientProtocolException, IOException, InterruptedException {
		WebHdfs webHdfs = Mockito.mock(WebHdfs.class);

		HttpResponse mkdirHttpResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(webHdfs.mkdir(Mockito.anyString())).thenReturn(mkdirHttpResponse);
		StatusLine mkdirStatusLine = Mockito.mock(StatusLine.class);
		Mockito.when(mkdirHttpResponse.getStatusLine()).thenReturn(mkdirStatusLine);
		Mockito.when(mkdirStatusLine.getStatusCode()).thenReturn(mkdirStatusCode);

		HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine = Mockito.mock(StatusLine.class);
		Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(fileStatusStatusCode);

		HttpResponse appendOrCreateHttpResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine1 = Mockito.mock(StatusLine.class);
		Mockito.when(appendOrCreateHttpResponse.getStatusLine()).thenReturn(statusLine1);
		Mockito.when(statusLine1.getStatusCode()).thenReturn(appendCreateStatusCode);

		Mockito.when(webHdfs.fileStatus(Mockito.anyString())).thenReturn(httpResponse);
		if (throwException)
			Mockito.when(webHdfs.createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class)))
					.thenThrow(Mockito.mock(IOException.class));
		else
			Mockito.when(webHdfs.createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class)))
					.thenReturn(appendOrCreateHttpResponse);
		Mockito.when(webHdfs.append(Mockito.anyString(), Mockito.any(InputStream.class)))
				.thenReturn(appendOrCreateHttpResponse);
		return webHdfs;
	}

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		Map<String, Object> properties = new HashMap<>();
		properties.put(WebHDFSWriterHandlerConstants.PORT, 1);
		properties.put(WebHDFSWriterHandlerConstants.HOST_NAMES, "host1");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH, "/webhdfs/${account}/{timestamp}");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_PREFIX, "raw-data");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_EXTENSION, ".txt");
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		WebHDFSWriterHandler.setPropertyMap(properties);
		WebHDFSWriterHandler.build();
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testBuildWithNullProperties() throws AdaptorConfigurationException {
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		WebHDFSWriterHandler.setPropertyMap(new HashMap<String, Object>());
		WebHDFSWriterHandler.build();
	}

	@Test
	public void testBuildWithHdfsPathLowerCase() throws AdaptorConfigurationException {
		Map<String, Object> properties = new HashMap<>();
		properties.put(WebHDFSWriterHandlerConstants.PORT, 1);
		properties.put(WebHDFSWriterHandlerConstants.HOST_NAMES, "host1");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH, "/webhDFs/${account}/{timestamp}");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_PREFIX, "raw-data");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH_LOWER_UPPER_CASE, "lower");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_EXTENSION, ".txt");
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		WebHDFSWriterHandler.setPropertyMap(properties);
		WebHDFSWriterHandler.build();
		Object hdfsPathLowerCase = ReflectionTestUtils.getField(WebHDFSWriterHandler, "hdfsPathCaseEnum");
		Assert.assertEquals(hdfsPathLowerCase, StringCase.LOWER);

	}

	@Test
	public void testBuildWithHdfsPathUpperCase() throws AdaptorConfigurationException {
		Map<String, Object> properties = new HashMap<>();
		properties.put(WebHDFSWriterHandlerConstants.PORT, 1);
		properties.put(WebHDFSWriterHandlerConstants.HOST_NAMES, "host1");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH, "/webhDFs/${account}/{timestamp}");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_PREFIX, "raw-data");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH_LOWER_UPPER_CASE, "upper");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_EXTENSION, ".txt");
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		WebHDFSWriterHandler.setPropertyMap(properties);
		WebHDFSWriterHandler.build();
		Object hdfsPathLowerCase = ReflectionTestUtils.getField(WebHDFSWriterHandler, "hdfsPathCaseEnum");
		Assert.assertEquals(hdfsPathLowerCase, StringCase.UPPER);
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "io.bigdime.core.InvalidValueConfigurationException: invalid value for hdfsPathCase, only.*")
	public void testBuildWithHdfsPathInvalidCase() throws AdaptorConfigurationException {
		Map<String, Object> properties = new HashMap<>();
		properties.put(WebHDFSWriterHandlerConstants.PORT, 1);
		properties.put(WebHDFSWriterHandlerConstants.HOST_NAMES, "host1");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH, "/webhDFs/${account}/{timestamp}");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_PREFIX, "raw-data");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH_LOWER_UPPER_CASE, "invalid");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_EXTENSION, ".txt");
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		WebHDFSWriterHandler.setPropertyMap(properties);
		WebHDFSWriterHandler.build();
	}

	@Test
	public void testBuildWithHdfsPathEmptyCase() throws AdaptorConfigurationException {
		Map<String, Object> properties = new HashMap<>();
		properties.put(WebHDFSWriterHandlerConstants.PORT, 1);
		properties.put(WebHDFSWriterHandlerConstants.HOST_NAMES, "host1");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH, "/webhDFs/${account}/{timestamp}");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_PREFIX, "raw-data");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_PATH_LOWER_UPPER_CASE, "");
		properties.put(WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_EXTENSION, ".txt");
		WebHDFSWriterHandler WebHDFSWriterHandler = new WebHDFSWriterHandler();
		WebHDFSWriterHandler.setPropertyMap(properties);
		WebHDFSWriterHandler.build();
		Object hdfsPathLowerCase = ReflectionTestUtils.getField(WebHDFSWriterHandler, "hdfsPathCaseEnum");
		Assert.assertEquals(hdfsPathLowerCase, StringCase.DEFAULT);
	}

	/**
	 * Assert that if there is a valid record_count available in
	 * RuntimeInfoStore and if the HandlerContext has events to process, all the
	 * events are processed and ActionEventHeaderConstants.RECORD_COUNT header
	 * is set to record_count from RuntimeInfoStore plus size of eventList from
	 * HandlerContext.
	 * 
	 * @throws HandlerException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws RuntimeInfoStoreException
	 */
	@Test
	public void testProcessWithValidValueFromRuntimeInfoStore() throws HandlerException, ClientProtocolException,
			IOException, InterruptedException, RuntimeInfoStoreException {

		WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		Map<String, String> properties = new HashMap<>();
		properties.put(ActionEventHeaderConstants.RECORD_COUNT, "1");
		runtimeInfo.setProperties(properties);
		Mockito.when(runtimeInfoStore.get(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
				.thenReturn(runtimeInfo);
		Random rand = new Random();
		int eventListSize = rand.nextInt(100);
		List<ActionEvent> actionEventList = new ArrayList<>();
		for (int i = 0; i < eventListSize; i++)
			actionEventList.add(mockActionEventWithHeaders());

		HttpResponse httpResponse = setupJsonResponse();
		Mockito.when(mockWebHdfs.append(Mockito.anyString(), Mockito.any(InputStream.class))).thenReturn(httpResponse);
		HandlerContext.get().setEventList(actionEventList);
		webHDFSWriterHandler.process();
		Assert.assertEquals(HandlerContext.get().getEventList().size(), 1,
				"WebHDFSWriterHandler should set only one event as a result of process");
		Assert.assertEquals(
				HandlerContext.get().getEventList().get(0).getHeaders().get(ActionEventHeaderConstants.RECORD_COUNT)
						.toString(),
				"" + (eventListSize + 1),
				"record_count header must be set and it should have be equal to record_count from RuntimeInfo plus eventList.size() from HandlerContext.");
		Assert.assertNull(HandlerContext.get().getJournal(webHDFSWriterHandler.getId()),
				"The journal must be cleared after processing");
	}

	@Test(expectedExceptions = SinkHandlerException.class)
	public void testProcessWithIllegalArgumentExceptionFromRuntimeInfoStore() throws HandlerException,
			ClientProtocolException, IOException, InterruptedException, RuntimeInfoStoreException {

		WebHDFSWriterHandler WebHDFSWriterHandler = mockWebHDFSWriterHandler();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		Map<String, String> properties = new HashMap<>();
		properties.put(ActionEventHeaderConstants.RECORD_COUNT, "1");
		runtimeInfo.setProperties(properties);

		Mockito.doThrow(Mockito.mock(IllegalArgumentException.class)).when(runtimeInfoStore).get(Mockito.anyString(),
				Mockito.anyString(), Mockito.anyString());

		ActionEvent actionEvent = mockActionEventWithHeaders();
		Mockito.doThrow(Mockito.mock(IOException.class)).when(mockWebHdfs).append(Mockito.anyString(),
				Mockito.any(InputStream.class));
		HandlerContext.get().createSingleItemEventList(actionEvent);
		WebHDFSWriterHandler.process();
	}

	/**
	 * Assert that if webhdfs is null, an instance is created. The process
	 * method must throw UnknownHostException, which would mean that webhdfs
	 * instance was created with an unknown host.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = UnknownHostException.class, expectedExceptionsMessageRegExp = "unit-testProcessWithWebhdfsNull.*")
	public void testProcessWithWebhdfsNull() throws Throwable {

		try {
			WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
			ReflectionTestUtils.setField(webHDFSWriterHandler, "webHdfs", null);
			ReflectionTestUtils.setField(webHDFSWriterHandler, "hostNames", "unit-testProcessWithWebhdfsNull");
			ReflectionTestUtils.setField(webHDFSWriterHandler, "port", 0);
			ActionEvent actionEvent = mockActionEventWithHeaders();
			HandlerContext.get().createSingleItemEventList(actionEvent);
			webHDFSWriterHandler.process();
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	@Test

	public void testProcessWithEventsFromThreePartitions()
			throws ClientProtocolException, IOException, InterruptedException, HandlerException {
		WebHDFSWriterHandler webHDFSWriterHandler = mockWebHDFSWriterHandler();
		List<ActionEvent> eventList = new ArrayList<>();
		Map<String, String> headers = new HashMap<>();

		ActionEvent event = new ActionEvent();
		headers.put("ACCOUNT", "mweb-us");
		headers.put("TIMESTAMP", "20151111");
		event.setHeaders(headers);
		eventList.add(event);

		headers = new HashMap<>();
		event = new ActionEvent();
		headers.put("ACCOUNT", "mweb-us");
		headers.put("TIMESTAMP", "20151111");
		event.setHeaders(headers);
		eventList.add(event);

		headers = new HashMap<>();
		event = new ActionEvent();
		headers.put("ACCOUNT", "mweb-us");
		headers.put("TIMESTAMP", "20151112");
		event.setHeaders(headers);
		eventList.add(event);

		headers = new HashMap<>();
		event = new ActionEvent();
		headers.put("ACCOUNT", "mweb-us");
		headers.put("TIMESTAMP", "20151112");
		event.setHeaders(headers);
		eventList.add(event);

		headers = new HashMap<>();
		event = new ActionEvent();
		headers.put("ACCOUNT", "mweb-us");
		headers.put("TIMESTAMP", "20151113");
		event.setHeaders(headers);
		eventList.add(event);

		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.setEventList(eventList);

		Status status = webHDFSWriterHandler.process();
		Mockito.verify(mockWebHdfs, Mockito.times(1)).mkdir("/webhdfs/mweb-us/20151111");
		Mockito.verify(mockWebHdfs, Mockito.times(1)).append(Mockito.eq("/webhdfs/mweb-us/20151111/unit-Channel"),
				Mockito.any(InputStream.class));
		Assert.assertSame(status, Status.CALLBACK);
		Assert.assertEquals(((WebHDFSWriterHandlerJournal) handlerContext.getJournal(webHDFSWriterHandler.getId()))
				.getEventList().size(), 3);
		//
		mockWebHdfs = mockWebHdfs(200, 200, 200, false);
		Mockito.doNothing().when(mockWebHdfs).releaseConnection();
		ReflectionTestUtils.setField(webHDFSWriterHandler, "webHdfs", mockWebHdfs);
		status = webHDFSWriterHandler.process();
		Mockito.verify(mockWebHdfs, Mockito.times(1)).mkdir("/webhdfs/mweb-us/20151112");
		Mockito.verify(mockWebHdfs, Mockito.times(1)).append(Mockito.eq("/webhdfs/mweb-us/20151112/unit-Channel"),
				Mockito.any(InputStream.class));
		Assert.assertSame(status, Status.CALLBACK);
		Assert.assertEquals(((WebHDFSWriterHandlerJournal) handlerContext.getJournal(webHDFSWriterHandler.getId()))
				.getEventList().size(), 1);
		//
		mockWebHdfs = mockWebHdfs(200, 200, 200, false);
		Mockito.doNothing().when(mockWebHdfs).releaseConnection();
		ReflectionTestUtils.setField(webHDFSWriterHandler, "webHdfs", mockWebHdfs);
		status = webHDFSWriterHandler.process();
		Mockito.verify(mockWebHdfs, Mockito.times(1)).mkdir("/webhdfs/mweb-us/20151113");
		Assert.assertNull(((WebHDFSWriterHandlerJournal) handlerContext.getJournal(webHDFSWriterHandler.getId())));
		Mockito.verify(mockWebHdfs, Mockito.times(1)).append(Mockito.eq("/webhdfs/mweb-us/20151113/unit-Channel"),
				Mockito.any(InputStream.class));
		Assert.assertSame(status, Status.READY);

	}

	private HttpResponse setupJsonResponse() throws UnsupportedOperationException, IOException {
		HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine = Mockito.mock(StatusLine.class);
		Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(200);

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

}
