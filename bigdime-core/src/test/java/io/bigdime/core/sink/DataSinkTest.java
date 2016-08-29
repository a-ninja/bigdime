/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.sink;

import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import org.apache.flume.lifecycle.LifecycleState;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.config.ADAPTOR_TYPE;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigReader;
import io.bigdime.core.handler.HandlerManager;

@Test(singleThreaded = true)
public class DataSinkTest {
	@Mock
	private LinkedHashSet<Handler> handlers;
	private AdaptorConfigReader adaptorConfigReader;

	@SuppressWarnings("unchecked")
	@BeforeMethod
	public void setUp() throws AdaptorConfigurationException {
		adaptorConfigReader = new AdaptorConfigReader();
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor.json");
		handlers = Mockito.mock(LinkedHashSet.class);
		Mockito.when(handlers.isEmpty()).thenReturn(false);
	}

	/**
	 * Assert that DataSink can not be constructed with a null handler list.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testConstructorWithNullHandlers() throws AdaptorConfigurationException {
		new DataSink(null, null);
	}

	/**
	 * Assert that DataSink can not be constructed with an empty handler list.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "null or empty collection not allowed for handlers")
	public void testConstructorWithEmptyHanders() throws AdaptorConfigurationException {
		new DataSink(new LinkedHashSet<Handler>(), null);
	}

	/**
	 * Verify that the process method on the handler is run when the sink is
	 * run. This test is for adaptor_type=Streaming. Verifies that hasNext on
	 * handler iterator has run atleast 3 times, meaning two loops.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws HandlerException
	 */
	//@Test
	public void testStartStreamingAdaptor() throws AdaptorConfigurationException, HandlerException {

		AdaptorConfig.getInstance().setType(ADAPTOR_TYPE.STREAMING);
		// adaptor.getAdaptorConfig().setType(ADAPTOR_TYPE.STREAMING);

		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler1 = Mockito.mock(Handler.class);
		Handler mockHandler2 = Mockito.mock(Handler.class);
		Handler mockHandler3 = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler1);
		handlerSet.add(mockHandler2);
		handlerSet.add(mockHandler3);
		Mockito.when(mockHandler1.getName()).thenReturn("mock-handler-1");
		Mockito.when(mockHandler2.getName()).thenReturn("mock-handler-2");
		Mockito.when(mockHandler3.getName()).thenReturn("mock-handler-3");

		DataSink dataSink = new DataSink(handlerSet, "unit-test-name");
		ReflectionTestUtils.setField(dataSink, "sleepForMillis", 10);
		// @SuppressWarnings("unchecked")
		// Iterator<Handler> handlerIterator = Mockito.mock(Iterator.class);
		// Mockito.when(handlers.iterator()).thenReturn(handlerIterator);
		// Handler mockHandler = Mockito.mock(Handler.class);
		// Mockito.when(mockHandler.process(Mockito.any(ActionEvent.class))).thenReturn(null);
		Mockito.when(mockHandler1.process()).thenReturn(Status.READY);
		Mockito.when(mockHandler2.process()).thenReturn(Status.CALLBACK);
		Mockito.when(mockHandler3.process()).thenReturn(Status.READY);
		// Mockito.when(handlerIterator.hasNext()).thenReturn(true).thenReturn(false);
		// Mockito.when(handlerIterator.next()).thenReturn(mockHandler);
		dataSink.start();

		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// Mockito.verify(mockHandler,
		// Mockito.times(1)).process(Mockito.any(ActionEvent.class));
		Mockito.verify(mockHandler1, Mockito.times(1)).process();
		Mockito.verify(mockHandler2, Mockito.atLeast(2)).process();
		Mockito.verify(mockHandler3, Mockito.atLeast(2)).process();

		// Mockito.verify(handlerIterator, Mockito.atLeast(3)).hasNext();
	}

	/**
	 * Assert that the HandlerManager.execute is invoked only once if the
	 * errorThreshold is set to 0 and the first invocation of execute throws an
	 * exception.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws HandlerException
	 */
	//@Test
	public void testStartWithHandlerManagerThrowingException() throws AdaptorConfigurationException, HandlerException {
		AdaptorConfig.getInstance().setType(ADAPTOR_TYPE.STREAMING);

		HandlerManager handlerManager = Mockito.mock(HandlerManager.class);
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler1 = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler1);
		Mockito.when(mockHandler1.getName()).thenReturn("mock-handler-1");

		DataSink dataSink = new DataSink(handlerSet, "unit-test-name");
		ReflectionTestUtils.setField(dataSink, "sleepForMillis", 10);
		ReflectionTestUtils.setField(dataSink, "errorThreshold", 0);
		ReflectionTestUtils.setField(dataSink, "handlerManager", handlerManager);
		Mockito.doThrow(HandlerException.class).when(handlerManager).execute();
		dataSink.start();

		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Mockito.verify(handlerManager, Mockito.times(1)).execute();
		Assert.assertEquals(dataSink.getLifecycleState(), LifecycleState.ERROR);
	}

	/**
	 * Assert that if the DataSink is stopped, the state of DataSink is set to
	 * STOP.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws HandlerException
	 */
	//@Test
	public void testStartAndStop() throws AdaptorConfigurationException, HandlerException {
		AdaptorConfig.getInstance().setType(ADAPTOR_TYPE.STREAMING);

		HandlerManager handlerManager = Mockito.mock(HandlerManager.class);
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler1 = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler1);
		Mockito.when(mockHandler1.getName()).thenReturn("mock-handler-1");

		DataSink dataSink = new DataSink(handlerSet, "unit-test-name");
		ReflectionTestUtils.setField(dataSink, "sleepForMillis", 10);
		ReflectionTestUtils.setField(dataSink, "errorThreshold", 0);
		ReflectionTestUtils.setField(dataSink, "handlerManager", handlerManager);
		Mockito.when(handlerManager.execute()).thenReturn(Status.BACKOFF);
		dataSink.start();
		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		dataSink.stop();
		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Mockito.verify(handlerManager, Mockito.atLeast(1)).execute();
		Assert.assertEquals(dataSink.getLifecycleState(), LifecycleState.STOP);
	}

	/**
	 * Verify that the process method on the handler is run when the sink is
	 * run. This test is for adaptor_type=BATCH. Just verifies that hasNext on
	 * handler iterator has run 2 times.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws HandlerException
	 */
	@Test
	public void testStartBatchAdaptor() throws AdaptorConfigurationException, HandlerException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		for (Handler h : handlerSet) {
			Mockito.when(h.process()).thenReturn(Status.READY);
		}
		Handler mockHandler = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler);
		AdaptorConfig.getInstance().setType(ADAPTOR_TYPE.BATCH);
		// adaptor.getAdaptorConfig().setType(ADAPTOR_TYPE.BATCH);
		DataSink dataSink = new DataSink(handlerSet, "unit-test-name");
		ReflectionTestUtils.setField(dataSink, "sleepForMillis", 100);
		// @SuppressWarnings("unchecked")
		// Iterator<Handler> handlerIterator = Mockito.mock(Iterator.class);
		// Mockito.when(handlers.iterator()).thenReturn(handlerIterator);

		// Mockito.when(handlerIterator.hasNext()).thenReturn(true);
		// Mockito.when(handlerIterator.next()).thenReturn(mockHandler);
		Mockito.when(mockHandler.process()).thenReturn(Status.READY).thenReturn(Status.BACKOFF);
		dataSink.start();

		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Mockito.verify(mockHandler, Mockito.atLeast(2)).process();
		// Mockito.verify(handlerIterator, Mockito.atLeast(2)).hasNext();
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testConstructorWithEmptyHandlers() throws AdaptorConfigurationException {
		new DataSink(new LinkedHashSet<Handler>(), null);
	}

	/**
	 * Assert that name obtained using getName method is equal to the one that
	 * was set using setName.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testNameGettersAndSetters() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSink dataSink = new DataSink(handlerSet, "unit-test-name");
		String actualName = dataSink.getName();
		Assert.assertSame(actualName, "unit-test-name");
		final String setName = UUID.randomUUID().toString();
		dataSink.setName(setName);
		Assert.assertEquals(dataSink.getName(), setName);
	}

	/**
	 * Assert that handler obtained using getHandlers method is equal to the one
	 * that was set using setHandlers.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testHandlersGettersAndSetters() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSink dataSink = new DataSink(handlerSet, "unit-test-testHandlersGettersAndSetters");
		LinkedHashSet<Handler> actualHandlers = dataSink.getHandlers();
		Assert.assertSame(actualHandlers, handlerSet);

		LinkedHashSet<Handler> setHandlers = new LinkedHashSet<>();
		dataSink.setHandlers(setHandlers);
		Assert.assertSame(dataSink.getHandlers(), setHandlers);
	}

	/**
	 * Assert that toString method is overridden by DataSink class and return a
	 * string starting with "DataSink [".
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testToString() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSink dataSink = new DataSink(handlerSet, "unit-test-testToString");
		Assert.assertTrue(dataSink.toString().startsWith("DataSink [handlers"));
	}

	/**
	 * Assert that getLifecycleState method is not implemented.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testGetLifecycleState() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		LifecycleState state = new DataSink(handlerSet, "name").getLifecycleState();
		Assert.assertEquals(state, LifecycleState.IDLE);
	}

	/**
	 * Assert that shutdown method invokes cancel method on futureTask and
	 * shutdown method on executorService. Also assert that shutdown method sets
	 * the interrupted flag to true on DataSink object.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testShutdown() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSink dataSink = new DataSink(handlerSet, "unit-test-name");
		HandlerManager handlerManager = Mockito.mock(HandlerManager.class);
		@SuppressWarnings("unchecked")
		FutureTask<Object> futureTask = Mockito.mock(FutureTask.class);
		ExecutorService executorService = Mockito.mock(ExecutorService.class);
		ReflectionTestUtils.setField(dataSink, "handlerManager", handlerManager);
		ReflectionTestUtils.setField(dataSink, "futureTask", futureTask);
		ReflectionTestUtils.setField(dataSink, "executorService", executorService);
		dataSink.stop();
		Object interrupted = ReflectionTestUtils.getField(dataSink, "interrupted");
		Assert.assertEquals(interrupted, true);
		Mockito.verify(futureTask, Mockito.times(1)).cancel(true);
		Mockito.verify(executorService, Mockito.times(1)).shutdown();
	}

	private LinkedHashSet<Handler> getRealHandlers() {
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler);
		return handlerSet;
	}

	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		DataSink dataSink = mockDataSink();
		dataSink.setName("unit-data-sink-name");
		dataSink.setDescription("unit-data-sink-description");
		Assert.assertTrue(dataSink.equals(dataSink));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		DataSink dataSink = mockDataSink();
		dataSink.setName("unit-data-sink-name");
		dataSink.setDescription("unit-data-sink-description");

		DataSink dataSink1 = mockDataSink();
		dataSink1.setName("unit-data-sink-name");
		dataSink1.setDescription("unit-data-sink-description-1");
		Assert.assertTrue(dataSink.equals(dataSink1));
		Assert.assertTrue(dataSink.hashCode() == dataSink1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		DataSink dataSink = mockDataSink();
		dataSink.setName("unit-data-sink-name-1");
		dataSink.setDescription("unit-data-sink-description");

		DataSink dataSink1 = mockDataSink();
		dataSink1.setName("unit-data-sink-name");
		dataSink1.setDescription("unit-data-sink-description");
		Assert.assertFalse(dataSink.equals(dataSink1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		DataSink dataSink = mockDataSink();
		dataSink.setName("unit-data-sink-name");
		dataSink.setDescription("unit-data-sink-description");
		Assert.assertFalse(dataSink.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		DataSink dataSink = mockDataSink();
		dataSink.setName("unit-data-sink-name");
		dataSink.setDescription("unit-data-sink-description");
		Assert.assertFalse(dataSink.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		DataSink dataSink = mockDataSink();
		dataSink.setName(null);
		dataSink.setDescription("unit-data-sink-description");

		DataSink dataSink1 = mockDataSink();
		dataSink1.setName("unit-data-sink-name");
		dataSink1.setDescription("unit-data-sink-description-1");
		Assert.assertFalse(dataSink.equals(dataSink1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		DataSink dataSink = mockDataSink();
		dataSink.setName(null);
		dataSink.setDescription("unit-data-sink-description");

		DataSink dataSink1 = mockDataSink();
		dataSink1.setName(null);
		dataSink1.setDescription("unit-data-sink-description-1");
		Assert.assertTrue(dataSink.equals(dataSink1));
		Assert.assertTrue(dataSink.hashCode() == dataSink1.hashCode());
	}

	private DataSink mockDataSink() {
		DataSink dataSink = null;
		try {
			LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
			Handler mockHandler1 = Mockito.mock(Handler.class);
			handlerSet.add(mockHandler1);
			dataSink = new DataSink(handlerSet, "unit-test-name");
		} catch (AdaptorConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataSink;
	}

	@Test
	public void testGetId() {
		DataSink dataSink = mockDataSink();
		Assert.assertNotNull(dataSink.getId());
	}

}
