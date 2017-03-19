/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.handler.HandlerFactoryTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

@Configuration
@ContextConfiguration({ "classpath*:application-context.xml", "classpath*:META-INF/application-context.xml" })

public class AdaptorConfigReaderTest extends AbstractTestNGSpringContextTests {
	@Autowired
	AdaptorConfigReader adaptorConfigReader;

	@BeforeMethod
	public void setUp() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor.json");
	}

	@AfterMethod
	public void tear() throws AdaptorConfigurationException {
	}

	public AdaptorConfigReaderTest() throws IOException {
		HandlerFactoryTest.initHandlerFactory();
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "com.fasterxml.jackson.databind.JsonMappingException:.*\\s*.*")
	public void testReadConfigInvalidFile() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/no-file");
		adaptorConfigReader.readConfig(AdaptorConfig.getInstance());
		Assert.fail("This method must have thrown exception");
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "com.fasterxml.jackson.core.io.JsonEOFException:.*\\s*.*")
	public void testReadConfigInvalidJson() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-bad.json");
		adaptorConfigReader.readConfig(AdaptorConfig.getInstance());
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testReadConfigPartialJson() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-partial.json");
		adaptorConfigReader.readConfig(AdaptorConfig.getInstance());
	}

	@Test
	public void dumbTest() {
		new AdaptorConfigReader();
	}

	@Test
	public void testReadConfigSaneConfigFile() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor.json");
		AdaptorConfig config = AdaptorConfig.getInstance();
		adaptorConfigReader.readConfig(config);
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "no sink found in adaptor configuration file")
	public void testReadConfigWithNoSink() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-no-sink.json");
		AdaptorConfig config = AdaptorConfig.getInstance();
		adaptorConfigReader.readConfig(config);
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "empty sink block is not allowed adaptor configuration file")
	public void testReadConfigWithEmptySink() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-empty-sink.json");
		AdaptorConfig config = AdaptorConfig.getInstance();
		adaptorConfigReader.readConfig(config);
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class, expectedExceptionsMessageRegExp = "empty source block is not allowed adaptor configuration file")
	public void testReadConfigWithEmptySource() throws AdaptorConfigurationException {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-empty-source.json");
		AdaptorConfig config = AdaptorConfig.getInstance();
		adaptorConfigReader.readConfig(config);
	}
}
