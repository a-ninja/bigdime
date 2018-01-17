/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.HandlerConfig;

@Configuration
@ContextConfiguration(classes = { HandlerFactory.class, DummyConcreteHandler.class, StringHelper.class })

public class HandlerFactoryTest extends AbstractTestNGSpringContextTests {
	@Autowired
	HandlerFactory handlerFactory;

	@Mock
	ApplicationContext context;

	@BeforeMethod
	public void initMocks() throws IOException {
		MockitoAnnotations.initMocks(this);
	}

	public HandlerFactoryTest() throws IOException {
		initHandlerFactory();
	}

	private static boolean initDone = false;

	public static synchronized void initHandlerFactory() throws IOException {
		if (initDone)
			return;
		System.setProperty("env.properties", "application.properties");
		initDone = true;
	}

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(handlerFactory);
	}

	@Test
	public void testHandlerFactory() throws IOException, AdaptorConfigurationException {
		new HandlerFactory();
	}

	// @Test(expectedExceptions = AdaptorConfigurationException.class,
	// expectedExceptionsMessageRegExp = "java.io.FileNotFoundException:.*(No
	// such file or directory).*")
	public void testHandlerFactoryWithIOException() throws IOException, AdaptorConfigurationException {
		System.setProperty("env.properties", "");
		new HandlerFactory();
	}

	@Test
	public void testGetHandler() throws AdaptorConfigurationException, IOException {

		ReflectionTestUtils.setField(handlerFactory, "context", context);
		HandlerConfig handlerConfig = new HandlerConfig();

		DummyConcreteHandler handler = Mockito.mock(DummyConcreteHandler.class);
		Mockito.when(context.getBean(DummyConcreteHandler.class)).thenReturn(handler);
		handlerConfig.setHandlerClass("io.bigdime.core.handler.DummyConcreteHandler");

		Map<String, Object> properties = new HashMap<>();
		handlerConfig.setHandlerProperties(properties);

		properties.put("unit-prop-1", "unit-prop-value-1");
		properties.put("unit-prop-2", new Object());
		properties.put("unit-prop-3", "${unit-token-1}");
		handlerFactory.getHandler(handlerConfig);
		Mockito.verify(handler, Mockito.times(1)).setName(Mockito.anyString());
		Mockito.verify(handler, Mockito.times(1)).setPropertyMap(properties);
		Mockito.verify(handler, Mockito.times(1)).build();
	}

	@Test
	public void testNegative() {
		try {
			HandlerConfig handlerConfig = Mockito.mock(HandlerConfig.class);
			Mockito.when(handlerConfig.getHandlerClass()).thenReturn("unit-handler-class");
			handlerFactory.getHandler(handlerConfig);
		} catch (AdaptorConfigurationException e) {
			Assert.assertTrue(e.getCause().getClass() == ClassNotFoundException.class);
		}
	}
}
