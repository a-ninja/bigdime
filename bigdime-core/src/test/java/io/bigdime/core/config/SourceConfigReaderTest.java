/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.RequiredParameterMissingConfigurationException;
import io.bigdime.core.commons.JsonHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Configuration
@ContextConfiguration(classes = { JsonHelper.class, HandlerConfigReader.class, SourceConfigReader.class })
public class SourceConfigReaderTest extends AbstractTestNGSpringContextTests {

	@Autowired
	SourceConfigReader sourceConfigReader;

	@Test(expectedExceptions = RequiredParameterMissingConfigurationException.class)
	public void testReadSource() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"src-desc\" : {},\"name\" : \"source-name\",\"description\" : \"source description\", \"source-type\" : \"file or mysql or oracle or kafka\",\"data-handlers\": []}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		sourceConfigReader.readSourceConfig(actualObj);
	}

	/**
	 * If the src-desc node is specified but has no values in it, SourceConfig
	 * should be configured with an empty map for srcDesc.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	@Test
	public void testReadSourceWithSrcDesc() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"src-desc\" : {\"input1\" : \"x:y\"},\"name\" : \"source-name\",\"description\" : \"source description\", \"source-type\" : \"file or mysql or oracle or kafka\",\"data-handlers\": []}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		SourceConfig sourceConfig = sourceConfigReader.readSourceConfig(actualObj);
		Assert.assertEquals(sourceConfig.getSrcDesc().size(), 1);
	}

	/**
	 * If the src-desc node is specified and has a nested node inside it,
	 * InvalidDataTypeConfigurationException must be thrown. Only text value is
	 * supported for src-desc.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testReadSourceWithSrcDescAsNotNode()
			throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"src-desc\" : {\"input1\" : {\"x\":\"y\"}},\"name\" : \"source-name\",\"description\" : \"source description\", \"source-type\" : \"file or mysql or oracle or kafka\",\"data-handlers\": []}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		SourceConfig sourceConfig = sourceConfigReader.readSourceConfig(actualObj);
		Assert.assertNotNull(sourceConfig);
		Assert.assertEquals(sourceConfig.getSrcDesc().size(), 1);
		Assert.assertNotNull(sourceConfig.getSrcDesc().get("input1"));
		Assert.assertSame(sourceConfig.getSrcDesc().get("input1").getClass(), java.util.HashMap.class);
		Assert.assertEquals(((Map<String, Object>) sourceConfig.getSrcDesc().get("input1")).get("x"), "y");
	}

	@Test
	public void testReadSourceWithHandlers()
			throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"src-desc\" : {\"input1\" : \"x:y\"},\"name\" : \"source-name\",\"description\" : \"source description\", \"source-type\" : \"file or mysql or oracle or kafka\",\"data-handlers\": [{     \"name\": \"kafka-data-reader\",     \"description\": \"read data from partitions specified with src-desc field\",     \"handler-class\": \"io.bigdime.core.handler.DummyConcreteHandler\",     \"properties\": {         \"broker-hosts\": \"kafka.provider.one:9092,kafka.provider.two:9096\",         \"offset-data-dir\": \"/tmp\",         \"message-size\" : \"20000\"}}]}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		SourceConfig sourceConfig = sourceConfigReader.readSourceConfig(actualObj);
		Assert.assertEquals(sourceConfig.getHandlerConfigs().size(), 1);

		Assert.assertEquals(sourceConfig.getSrcDesc().size(), 1);
		Assert.assertNotNull(sourceConfig.getSrcDesc().get("input1"));
		Assert.assertSame(sourceConfig.getSrcDesc().get("input1").getClass(), String.class);
		System.out.println(sourceConfig.getSrcDesc());
		Assert.assertEquals((sourceConfig.getSrcDesc().get("input1")), "x:y");
	}

}
