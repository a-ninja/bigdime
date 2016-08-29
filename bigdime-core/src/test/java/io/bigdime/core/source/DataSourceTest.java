/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.source;

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
import io.bigdime.core.handler.HandlerManager;

public class DataSourceTest {

	@Mock
	private LinkedHashSet<Handler> handlers;

	@SuppressWarnings("unchecked")
	@BeforeMethod
	public void setUp() {
		handlers = Mockito.mock(LinkedHashSet.class);
		Mockito.when(handlers.isEmpty()).thenReturn(false);
	}

	/**
	 * Assert that DataSource can not be constructed with a null handler list.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "null or empty collection not allowed for handlers")
	public void testConstructorWithNullHanders() throws AdaptorConfigurationException {
		new DataSource(null, null);
	}

	/**
	 * Assert that DataSource can not be constructed with an empty handler list.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "null or empty collection not allowed for handlers")
	public void testConstructorWithEmptyHanders() throws AdaptorConfigurationException {
		new DataSource(new LinkedHashSet<Handler>(), null);
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
		DataSource dataSource = new DataSource(handlerSet, "unit-test-testNameGettersAndSetters");
		String actualName = dataSource.getName();
		Assert.assertSame(actualName, "unit-test-testNameGettersAndSetters");
		final String setName = UUID.randomUUID().toString();
		dataSource.setName(setName);
		Assert.assertEquals(dataSource.getName(), setName);
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
		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		LinkedHashSet<Handler> actualHandlers = dataSource.getHandlers();
		Assert.assertSame(actualHandlers, handlerSet);

		LinkedHashSet<Handler> setHandlers = new LinkedHashSet<>();
		dataSource.setHandlers(setHandlers);
		Assert.assertSame(dataSource.getHandlers(), setHandlers);
	}

	/**
	 * Assert that toString method is overridden by DataSource class and return
	 * a string starting with "DataSource [".
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testToString() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		Assert.assertTrue(dataSource.toString().startsWith("DataSource [handlers"));
	}

	/**
	 * Assert that getLifecycleState method is not implemented.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testGetLifecycleState() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		Assert.assertEquals(new DataSource(handlerSet, "name").getLifecycleState(), LifecycleState.IDLE);

	}

	private LinkedHashSet<Handler> getRealHandlers() {
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler);
		return handlerSet;
	}

	/**
	 * Assert that shutdown method invokes cancel method on futureTask and
	 * shutdown method on executorService. Also assert that shutdown method sets
	 * the interrupted flag to true on DataSource object.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testShutdown() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		HandlerManager handlerManager = Mockito.mock(HandlerManager.class);
		FutureTask<?> futureTask = Mockito.mock(FutureTask.class);

		ExecutorService executorService = Mockito.mock(ExecutorService.class);
		ReflectionTestUtils.setField(dataSource, "handlerManager", handlerManager);
		ReflectionTestUtils.setField(dataSource, "futureTask", futureTask);
		ReflectionTestUtils.setField(dataSource, "executorService", executorService);
		dataSource.stop();
		Object interrupted = ReflectionTestUtils.getField(dataSource, "interrupted");
		Assert.assertEquals(interrupted, true);
		Mockito.verify(futureTask, Mockito.times(1)).cancel(true);
		Mockito.verify(executorService, Mockito.times(1)).shutdown();
	}

	/**
	 * Assert that start method can be invoked and doesn't throw any exception.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws InterruptedException
	 */
	@Test
	public void testStartWithNullAdaptorType() throws AdaptorConfigurationException, InterruptedException {
		LinkedHashSet<Handler> handlerSet = getRealHandlers();
		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		dataSource.start();
	}

	/**
	 * Assert that the HandlerManager.execute is invoked only once if the
	 * errorThreshold is set to 0 and the first invocation of execute throws an
	 * exception.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws HandlerException
	 */
	@Test
	public void testStartWithHandlerManagerThrowingException() throws AdaptorConfigurationException, HandlerException {
		AdaptorConfig.getInstance().setType(ADAPTOR_TYPE.STREAMING);

		HandlerManager handlerManager = Mockito.mock(HandlerManager.class);
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler1 = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler1);
		Mockito.when(mockHandler1.getName()).thenReturn("mock-handler-1");

		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		ReflectionTestUtils.setField(dataSource, "sleepForMillis", 10);
		ReflectionTestUtils.setField(dataSource, "errorThreshold", 0);
		ReflectionTestUtils.setField(dataSource, "handlerManager", handlerManager);
		Mockito.doThrow(HandlerException.class).when(handlerManager).execute();
		dataSource.start();

		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Mockito.verify(handlerManager, Mockito.times(1)).execute();
		Assert.assertEquals(dataSource.getLifecycleState(), LifecycleState.ERROR);
	}

	/**
	 * Assert that if the DataSource is stopped, the state of DataSource is set
	 * to STOP.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws HandlerException
	 */
	@Test
	public void testStartAndStop() throws AdaptorConfigurationException, HandlerException {
		AdaptorConfig.getInstance().setType(ADAPTOR_TYPE.STREAMING);

		HandlerManager handlerManager = Mockito.mock(HandlerManager.class);
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler1 = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler1);
		Mockito.when(mockHandler1.getName()).thenReturn("mock-handler-1");

		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		ReflectionTestUtils.setField(dataSource, "sleepForMillis", 10);
		ReflectionTestUtils.setField(dataSource, "errorThreshold", 0);
		ReflectionTestUtils.setField(dataSource, "handlerManager", handlerManager);
		Mockito.when(handlerManager.execute()).thenReturn(Status.BACKOFF);
		dataSource.start();
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		dataSource.stop();
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Mockito.verify(handlerManager, Mockito.atLeast(1)).execute();
		Assert.assertEquals(dataSource.getLifecycleState(), LifecycleState.STOP);
	}

	@Test
	public void testUpdate() throws AdaptorConfigurationException {
		LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
		Handler mockHandler1 = Mockito.mock(Handler.class);
		handlerSet.add(mockHandler1);
		DataSource dataSource = new DataSource(handlerSet, "unit-test-name");
		ExecutorService executorService = Mockito.mock(ExecutorService.class);
		ReflectionTestUtils.setField(dataSource, "executorService", executorService);

		dataSource.update(null, dataSource);
		Mockito.verify(executorService, Mockito.times(1)).shutdown();
	}

	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		DataSource dataSource = mockDataSource();
		dataSource.setName("unit-data-source-name");
		dataSource.setDescription("unit-data-source-description");
		Assert.assertTrue(dataSource.equals(dataSource));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		DataSource dataSource = mockDataSource();
		dataSource.setName("unit-data-source-name");
		dataSource.setDescription("unit-data-source-description");

		DataSource dataSource1 = mockDataSource();
		dataSource1.setName("unit-data-source-name");
		dataSource1.setDescription("unit-data-source-description-1");
		Assert.assertTrue(dataSource.equals(dataSource1));
		Assert.assertTrue(dataSource.hashCode() == dataSource1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		DataSource dataSource = mockDataSource();
		dataSource.setName("unit-data-source-name-1");
		dataSource.setDescription("unit-data-source-description");

		DataSource dataSource1 = mockDataSource();
		dataSource1.setName("unit-data-source-name");
		dataSource1.setDescription("unit-data-source-description");
		Assert.assertFalse(dataSource.equals(dataSource1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		DataSource dataSource = mockDataSource();
		dataSource.setName("unit-data-source-name");
		dataSource.setDescription("unit-data-source-description");
		Assert.assertFalse(dataSource.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		DataSource dataSource = mockDataSource();
		dataSource.setName("unit-data-source-name");
		dataSource.setDescription("unit-data-source-description");
		Assert.assertFalse(dataSource.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		DataSource dataSource = mockDataSource();
		dataSource.setName(null);
		dataSource.setDescription("unit-data-source-description");

		DataSource dataSource1 = mockDataSource();
		dataSource1.setName("unit-data-source-name");
		dataSource1.setDescription("unit-data-source-description-1");
		Assert.assertFalse(dataSource.equals(dataSource1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		DataSource dataSource = mockDataSource();
		dataSource.setName(null);
		dataSource.setDescription("unit-data-source-description");

		DataSource dataSource1 = mockDataSource();
		dataSource1.setName(null);
		dataSource1.setDescription("unit-data-source-description-1");
		Assert.assertTrue(dataSource.equals(dataSource1));
		Assert.assertTrue(dataSource.hashCode() == dataSource1.hashCode());
	}

	private DataSource mockDataSource() {
		DataSource dataSource = null;
		try {
			LinkedHashSet<Handler> handlerSet = new LinkedHashSet<>();
			Handler mockHandler1 = Mockito.mock(Handler.class);
			handlerSet.add(mockHandler1);
			dataSource = new DataSource(handlerSet, "unit-test-name");
		} catch (AdaptorConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataSource;
	}

	@Test
	public void testGetId() {
		DataSource dataSource = mockDataSource();
		Assert.assertNotNull(dataSource.getId());
	}
}
