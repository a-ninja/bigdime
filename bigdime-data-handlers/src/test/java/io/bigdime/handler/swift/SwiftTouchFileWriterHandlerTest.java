package io.bigdime.handler.swift;

import static org.mockito.Matchers.anyChar;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;

public class SwiftTouchFileWriterHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		Map<String, Object> properties = new HashMap<>();
		properties.put(SwiftWriterHandlerConstants.USER_NAME, "unit-swift-user-name");
		properties.put(SwiftWriterHandlerConstants.PASSWORD, "unit-swift-password");
		properties.put(SwiftWriterHandlerConstants.AUTH_URL, "unit-swift-auth-url");
		properties.put(SwiftWriterHandlerConstants.TENANT_ID, "unit-swift-tenant-id");
		properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name");
		properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name");
		properties.put(SwiftWriterHandlerConstants.CONTAINER_NAME, "unit-swift-container-name");

		properties.put(SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN,
				"\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/([\\w\\-]+)\\/(\\w+)\\/([\\w\\.]+)$");
		properties.put(SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN, "$1__$2/$3");

		SwiftByteWriterHandler swiftByteWriterHandler = new SwiftByteWriterHandler();
		swiftByteWriterHandler.setPropertyMap(properties);
		swiftByteWriterHandler.build();
	}

	@Test(threadPoolSize = 1)
	public void testProcessWithNoEventsInContext() throws Throwable {
		String writeData = "unit-test-testProcess";
		List<ActionEvent> contextEventList = new ArrayList<>();
		ReturnStatus returnStatus = setupHandler(writeData, contextEventList, getDefaultProperties());
		Assert.assertEquals(returnStatus.getStatus(), Status.BACKOFF);
		List<ActionEvent> actionEvents = returnStatus.getEventList();
		Assert.assertNull(actionEvents);
	}

	@Test(threadPoolSize = 1)
	public void testProcessWithOneEventInContext() throws Throwable {
		String writeData = "unit-test-testProcess";
		List<ActionEvent> contextEventList = getTestEvents(1);

		ReturnStatus returnStatus = setupHandler(writeData, contextEventList, getDefaultProperties());
		Assert.assertEquals(returnStatus.getStatus(), Status.READY);

		List<ActionEvent> actionEvents = returnStatus.getEventList();
		Assert.assertNull(actionEvents);
	}

	@Test(threadPoolSize = 1)
	public void testProcessWithEventsInContextAndNotLastHandler() throws Throwable {
		String writeData = "unit-test-testProcess";
		List<ActionEvent> contextEventList = getTestEvents(1);

		final Map<String, Object> properties = getDefaultProperties();
		properties.remove(ActionEventHeaderConstants.LAST_HANDLER_IN_CHAIN);
		ReturnStatus returnStatus = setupHandler(writeData, contextEventList, properties);
		Assert.assertEquals(returnStatus.getStatus(), Status.READY);

		List<ActionEvent> actionEvents = returnStatus.getEventList();
		Assert.assertNotNull(actionEvents);
	}

	@Test(threadPoolSize = 1)
	public void testProcessWithEventsInContext() throws Throwable {
		String writeData = "unit-test-testProcess";
		List<ActionEvent> contextEventList = getTestEvents(2);

		ReturnStatus returnStatus = setupHandler(writeData, contextEventList, getDefaultProperties());
		Assert.assertEquals(returnStatus.getStatus(), Status.CALLBACK);
		List<ActionEvent> actionEvents = returnStatus.getEventList();
		System.out.println("actionEvents=" + actionEvents);
		Assert.assertNull(actionEvents);
	}

	class ReturnStatus {
		private Status status;
		private List<ActionEvent> eventList = new ArrayList<>();

		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}

		public List<ActionEvent> getEventList() {
			return eventList;
		}

		public void setEventList(List<ActionEvent> eventList) {
			this.eventList = eventList;
		}

	}

	public ReturnStatus setupHandler(final String writeData, final List<ActionEvent> contextEventList,
			final Map<String, Object> properties) throws Throwable {
		FutureTask<ReturnStatus> futureTask = new FutureTask<>(new Callable<ReturnStatus>() {
			@Override
			public ReturnStatus call() throws Exception {
				SwiftTouchFileWriterHandler swiftTouchFileWriterHandler = createSwiftTouchFileWriterHandler(properties);
				ReflectionTestUtils.setField(swiftTouchFileWriterHandler, "initialized", true);
				HandlerContext context = HandlerContext.get();
				HandlerJournal journal = new SwiftWriterHandlerJournal();
				context.setJournal(swiftTouchFileWriterHandler.getId(), journal);
				context.setEventList(contextEventList);
				Status status = swiftTouchFileWriterHandler.process();

				ReturnStatus returnStatus = new ReturnStatus();
				returnStatus.setStatus(status);
				returnStatus.setEventList(context.getEventList());
				System.out.println("context in thread=" + context + "," + context.getEventList());
				return returnStatus;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	private Map<String, Object> getDefaultProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(SwiftWriterHandlerConstants.USER_NAME, "unit-swift-user-name");
		properties.put(SwiftWriterHandlerConstants.PASSWORD, "unit-swift-password");
		properties.put(SwiftWriterHandlerConstants.AUTH_URL, "unit-swift-auth-url");
		properties.put(SwiftWriterHandlerConstants.TENANT_ID, "unit-swift-tenant-id");
		properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name");
		properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name");
		properties.put(SwiftWriterHandlerConstants.CONTAINER_NAME, "unit-swift-container-name");
		properties.put(ActionEventHeaderConstants.LAST_HANDLER_IN_CHAIN, "true");

		properties.put(SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN,
				"\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/([\\w\\-]+)\\/(\\w+)\\/([\\w\\.]+)$");
		properties.put(SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN, "touchfiles/$1__$2");
		properties.put(SwiftWriterHandlerConstants.FILE_PATH_PREFIX_PATTERN, "$1__$2/");
		properties.put(SwiftWriterHandlerConstants.FILE_PATH_PATTERN, "$1__$2/$3");
		return properties;
	}

	private SwiftTouchFileWriterHandler createSwiftTouchFileWriterHandler(Map<String, Object> properties)
			throws AdaptorConfigurationException {
		Container mockContainer = Mockito.mock(Container.class);
		Collection<DirectoryOrObject> mockSwiftDirListing = new ArrayList<>();

		DirectoryOrObject dirOrObject1 = Mockito.mock(DirectoryOrObject.class);
		Mockito.when(dirOrObject1.getName()).thenReturn("20160101__entityName/fileName01.ext");
		DirectoryOrObject dirOrObject2 = Mockito.mock(DirectoryOrObject.class);
		Mockito.when(dirOrObject2.getName()).thenReturn("20160101__entityName/fileName02.ext");
		mockSwiftDirListing.add(dirOrObject1);
		mockSwiftDirListing.add(dirOrObject2);
		Mockito.when(mockContainer.listDirectory(anyString(), anyChar(), anyString(), anyInt()))
				.thenReturn(mockSwiftDirListing);

		StoredObject storedObject = Mockito.mock(StoredObject.class);

		Mockito.when(mockContainer.getObject(anyString())).thenReturn(storedObject);

		SwiftTouchFileWriterHandler swiftTouchFileWriterHandler = new SwiftTouchFileWriterHandler();
		swiftTouchFileWriterHandler.setPropertyMap(properties);
		swiftTouchFileWriterHandler.build();

		ReflectionTestUtils.setField(swiftTouchFileWriterHandler, "container", mockContainer);
		return swiftTouchFileWriterHandler;
	}

	private List<ActionEvent> getTestEvents(int num) {
		List<ActionEvent> contextEventList = new ArrayList<>(num);
		for (int i = 0; i < num; i++) {
			ActionEvent event = new ActionEvent();
			event.setBody(
					("/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName01.ext\n/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName0"
							+ num + ".ext").getBytes());

			event.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME,
					"/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName01.ext");
			contextEventList.add(event);
		}
		return contextEventList;
	}
}
