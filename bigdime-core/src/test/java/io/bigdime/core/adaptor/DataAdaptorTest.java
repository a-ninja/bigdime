/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.AdaptorContext;
import io.bigdime.core.AdaptorPhase;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.Sink;
import io.bigdime.core.Source;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigReader;
import io.bigdime.core.handler.HandlerFactoryTest;
import org.apache.flume.lifecycle.LifecycleState;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

@Configuration
@ContextConfiguration({ "classpath*:application-context.xml", "classpath*:META-INF/application-context.xml" })

public class DataAdaptorTest extends AbstractTestNGSpringContextTests {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataAdaptorTest.class));

	@Autowired
	DataAdaptor dataAdaptor;

	@Autowired
	AdaptorConfigReader adaptorConfigReader;

	public DataAdaptorTest() throws IOException {
		HandlerFactoryTest.initHandlerFactory();
	}

	@BeforeMethod
	public void setup() throws Exception {
		MockitoAnnotations.initMocks(this);
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor.json");
	}

//	@Test
//	public void testSampleController() {
//		SpringApplication.run(SampleController.class, "");
//	}
	// @Test(priority=1)
	public void testStart() throws DataAdaptorException {
		logger.info("unit-test", "Starting DataAdaptor");
		// dataAdaptor.getAdaptorConfig().setType(ADAPTOR_TYPE.BATCH);
		for (Source source : dataAdaptor.getSources()) {
			ReflectionTestUtils.setField(source, "sleepForMillis", 100);
		}
		for (Sink sink : dataAdaptor.getSinks()) {
			ReflectionTestUtils.setField(sink, "sleepForMillis", 100);
		}
		new Thread() {
			public void run() {
				try {
					dataAdaptor.start();
				} catch (DataAdaptorException e) {
					Assert.fail(e.getMessage());
				}
			}
		}.start();

		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.info("unit-test", "Stopping DataAdaptor");
		dataAdaptor.stop();
		// no assert, no exceptions
	}

	/**
	 * Make sure that start method in Source and Sink is invoked when adaptor is
	 * started.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(priority = 1)
	public void testStartWithMockData() throws DataAdaptorException {
		logger.info("unit-test", "Starting DataAdaptor");
		setMockAdaptor();
		Mockito.when(
				dataAdaptor.getAdaptorConfig().getAdaptorContext().getSources().iterator().next().getLifecycleState())
				.thenReturn(LifecycleState.STOP);
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.INIT);
		dataAdaptor.start();
		Mockito.verify(dataAdaptor.getAdaptorConfig().getAdaptorContext().getSources().iterator().next(),
				Mockito.times(1)).start();
		Mockito.verify(dataAdaptor.getAdaptorConfig().getAdaptorContext().getSinks().iterator().next(),
				Mockito.times(1)).start();
	}

	/**
	 * Make sure that DataAdaptorException is thrown when we try to start an
	 * adaptor when it's in STARTING phase.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(priority = 2)
	public void testStartFromStartingState() throws DataAdaptorException {
		logger.info("unit-test", "Starting DataAdaptor");
		setMockAdaptor();
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.STARTING);
		Assert.assertFalse(dataAdaptor.start());
	}

	/**
	 * Make sure that DataAdaptorException is thrown when we try to start an
	 * adaptor when it's in STARTED phase.
	 * 
	 * @throws DataAdaptorException
	 *             TODO: refactor this method to another class to run start and
	 *             stop tests in sequence.
	 */
	// @Test(priority = 3)
	public void testStartFromStartedState() throws DataAdaptorException {
		logger.info("unit-test", "Starting DataAdaptor");
		setMockAdaptor();
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.STARTED);
		Assert.assertFalse(dataAdaptor.start());
	}

	/**
	 * Make sure that if the adaptor is in STARTED phase, it can be stopped.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(priority = 4)
	public void testStop() throws DataAdaptorException {
		setMockAdaptor();
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.STARTED);
		dataAdaptor.stop();
		Mockito.verify(dataAdaptor.getAdaptorConfig().getAdaptorContext().getSources().iterator().next(),
				Mockito.times(1)).stop();
		Mockito.verify(dataAdaptor.getAdaptorConfig().getAdaptorContext().getSinks().iterator().next(),
				Mockito.times(1)).stop();
	}

	/**
	 * Make sure that if the adaptor is in STARTING phase, it can be stopped.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(priority = 5)
	public void testStopStartingADaptor() throws DataAdaptorException {
		setMockAdaptor();
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.STARTING);
		dataAdaptor.stop();
		Mockito.verify(dataAdaptor.getAdaptorConfig().getAdaptorContext().getSources().iterator().next(),
				Mockito.times(1)).stop();
		Mockito.verify(dataAdaptor.getAdaptorConfig().getAdaptorContext().getSinks().iterator().next(),
				Mockito.times(1)).stop();
	}

	/**
	 * Make sure that DataAdaptorException is thrown if we try to stop an
	 * adaptor that's in STOPPED status.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(expectedExceptions = DataAdaptorException.class, priority = 6)
	public void testStopStoppedADaptor() throws DataAdaptorException {
		setMockAdaptor();
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.STOPPED);
		dataAdaptor.stop();
	}

	/**
	 * Make sure that DataAdaptorException is thrown if we try to stop an
	 * adaptor that's in STOPPING status.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(expectedExceptions = DataAdaptorException.class, priority = 7)
	public void testStopStoppingADaptor() throws DataAdaptorException {
		setMockAdaptor();
		ReflectionTestUtils.setField(dataAdaptor, "adaptorCurrentPhase", AdaptorPhase.STOPPING);
		dataAdaptor.stop();
	}

	@Test(priority = 8)
	public void testInitWithStartAutoStartTrue() throws DataAdaptorException, SchedulerException {
		setMockAdaptor();
		DataAdaptorJob mockJob = Mockito.mock(DataAdaptorJob.class);
		ReflectionTestUtils.setField(dataAdaptor, "dataAdaptorJob", mockJob);
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION",
				"META-INF/adaptor-streaming-auto-start-true.json");
		ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
		Mockito.verify(mockJob, Mockito.times(1)).scheduleStreamingJob(dataAdaptor);
		Mockito.verify(mockJob, Mockito.times(0)).scheduleBatchJob(Mockito.any(DataAdaptor.class));
	}

	@Test(priority = 9)
	public void testInitWithBatchAutoStartTrue() throws DataAdaptorException, SchedulerException {
		setMockAdaptor();
		DataAdaptorJob mockJob = Mockito.mock(DataAdaptorJob.class);
		ReflectionTestUtils.setField(dataAdaptor, "dataAdaptorJob", mockJob);
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION",
				"META-INF/adaptor-batch-auto-start-true.json");
		ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
		Mockito.verify(mockJob, Mockito.times(1)).scheduleBatchJob(dataAdaptor);
		Mockito.verify(mockJob, Mockito.times(0)).scheduleStreamingJob(Mockito.any(DataAdaptor.class));
	}

	@Test(priority = 10)
	public void testInitWithBatchAutoStartFalse() throws DataAdaptorException, SchedulerException {
		setMockAdaptor();
		DataAdaptorJob mockJob = Mockito.mock(DataAdaptorJob.class);
		ReflectionTestUtils.setField(dataAdaptor, "dataAdaptorJob", mockJob);
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-batch.json");
		ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
		Mockito.verify(mockJob, Mockito.times(0)).scheduleBatchJob(Mockito.any(DataAdaptor.class));
		Mockito.verify(mockJob, Mockito.times(0)).scheduleStreamingJob(Mockito.any(DataAdaptor.class));
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, priority = 11)
	public void testInitWithAdaptorConfigurationException() throws Throwable {
		AdaptorConfigReader mockReader = Mockito.mock(AdaptorConfigReader.class);
		ReflectionTestUtils.setField(dataAdaptor, "adaptorConfigReader", mockReader);
		Mockito.doThrow(AdaptorConfigurationException.class).when(mockReader)
				.readConfig(Mockito.any(AdaptorConfig.class));
		try {
			ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	@Test(priority = 12)
	public void testInitWithException() throws Throwable {
		AdaptorConfigReader mockReader = Mockito.mock(AdaptorConfigReader.class);
		ReflectionTestUtils.setField(dataAdaptor, "adaptorConfigReader", mockReader);
		Mockito.doThrow(Exception.class).when(mockReader).readConfig(Mockito.any(AdaptorConfig.class));
		try {
			ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
			Assert.fail("Must have thrown an IllegalArgumentException");
		} catch (Exception e) {
			Assert.assertTrue(e.getClass() == IllegalArgumentException.class);
		}
	}

	@Test
	public void testReadConfig() throws AdaptorConfigurationException {
		AdaptorConfig adaptorConfig1 = dataAdaptor.getAdaptorConfig();
		Assert.assertNotNull(adaptorConfig1);
		Assert.assertNotNull(adaptorConfig1.getName(), "adaptor name can not be null");
		Assert.assertNotNull(adaptorConfig1.getType(), "adaptor type can not be null");
		// todo: assert other values too.
	}

	/**
	 * Assert that AdaptorConfigurationException is thrown if there are no
	 * source configured.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigFileWithNoSource() throws Throwable {
		try {
			dataAdaptor.getAdaptorConfig().setSourceConfig(null);
			ReflectionTestUtils.invokeMethod(dataAdaptor, "initializeSource");
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	/**
	 * Assert that AdaptorConfigurationException is thrown if there are no sinks
	 * configured.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigFileWithNoSink() throws Throwable {
		try {
			dataAdaptor.getAdaptorConfig().getSinkConfigs().clear();
			ReflectionTestUtils.invokeMethod(dataAdaptor, "initializeSinks");
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	/**
	 * input-processor doesn't map the input to any channel.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigForNoChannelMappedForInput() throws Throwable {
		try {
			ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION",
					"META-INF/adaptor-no-channel-mapped-for-input.json");
			ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
		} catch (Exception ex) {
			throw ex.getCause();
		}
	}

	/**
	 * input-processor must contain a channel map in input:channel format.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigForInvalidChannelMapValue() throws Throwable {
		try {
			synchronized (DataAdaptor.class) {
				ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION",
						"META-INF/adaptor-invalid-channel-map-value.json");
				adaptorConfigReader.readConfig(AdaptorConfig.getInstance());
				ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
			}
		} catch (Exception ex) {
			throw ex.getCause();
		}
	}

	/**
	 * When the input processor uses a channel that's not present in channels
	 * section.
	 * 
	 * @throws DataAdaptorException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigForMappedChannelNotAvailable() throws Throwable {
		try {
			ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION",
					"META-INF/adaptor-mapped-channel-not-available.json");
			ReflectionTestUtils.invokeMethod(dataAdaptor, "init");
		} catch (Exception ex) {
			throw ex.getCause();
		}
	}

	/**
	 * Assert that AdaptorConfigurationException is thrown if there are no
	 * channels configured.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigFileWithNoChannels() throws Throwable {
		try {
			dataAdaptor.getAdaptorConfig().getChannelConfigs().clear();
			ReflectionTestUtils.invokeMethod(dataAdaptor, "initializeChannels");
		} catch (Exception e) {
			throw e.getCause();
		}
	}

	private void setMockAdaptor() {
		AdaptorConfig adaptorConfig = AdaptorConfig.getInstance();
		AdaptorContext adaptorcontext = adaptorConfig.getAdaptorContext();

		// set source
		@SuppressWarnings("unchecked")
		Collection<Source> mockSources = Mockito.mock(HashSet.class);
		@SuppressWarnings("unchecked")
		Iterator<Source> mockiterator = Mockito.mock(Iterator.class);
		Mockito.when(mockSources.iterator()).thenReturn(mockiterator);
		Mockito.when(mockiterator.hasNext()).thenReturn(true).thenReturn(false);
		Source mockSource = Mockito.mock(Source.class);
		Mockito.when(mockSource.getLifecycleState()).thenReturn(LifecycleState.START);
		Mockito.when(mockiterator.next()).thenReturn(mockSource);
		adaptorcontext.setSources(mockSources);

		// Set sinks
		@SuppressWarnings("unchecked")
		Collection<Sink> mockSinks = Mockito.mock(HashSet.class);
		@SuppressWarnings("unchecked")
		Iterator<Sink> mockSinkIterator = Mockito.mock(Iterator.class);
		Mockito.when(mockSinks.iterator()).thenReturn(mockSinkIterator);
		Mockito.when(mockSinkIterator.hasNext()).thenReturn(true).thenReturn(false);
		Sink mockSink = Mockito.mock(Sink.class);
		Mockito.when(mockSinkIterator.next()).thenReturn(mockSink);
		adaptorcontext.setSinks(mockSinks);

	}

}
