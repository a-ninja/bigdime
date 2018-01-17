/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.InvalidDataTypeConfigurationException;
import io.bigdime.core.commons.JsonHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

@Configuration
@ContextConfiguration(classes = { JsonHelper.class, SinkConfigReader.class, HandlerConfigReader.class })
public class SinkConfigReaderTest extends AbstractTestNGSpringContextTests {

	@Autowired
	SinkConfigReader sinkConfigReader;

	/**
	 * If the channel-desc node is specified and has a nested node inside it,
	 * InvalidDataTypeConfigurationException must be thrown. Only text value is
	 * supported for channel-desc.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	@Test
	public void testReadSink() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"channel-desc\" : [\"channel1\"],\"name\" : \"source-name\",\"description\" : \"source description\",\"data-handlers\": []}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		sinkConfigReader.readSinkConfig(actualObj);
	}

	/**
	 * If the channel-desc node is specified and has a nested node inside it,
	 * InvalidDataTypeConfigurationException must be thrown. Only text value is
	 * supported for channel-desc.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	@Test(expectedExceptions = InvalidDataTypeConfigurationException.class)
	public void testReadSinkWithEmptyChannelDesc()
			throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"channel-desc\" : [{}],\"name\" : \"sink-name\",\"description\" : \"sink description\",\"data-handlers\": []}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		sinkConfigReader.readSinkConfig(actualObj);
	}

	/**
	 * If the channel-desc node is specified and has a nested node inside it,
	 * InvalidDataTypeConfigurationException must be thrown. Only text value is
	 * supported for channel-desc.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	@Test(expectedExceptions = InvalidDataTypeConfigurationException.class, expectedExceptionsMessageRegExp = "channel-desc must be of text node")
	public void testReadSinkWithChannelDescAsNotNode()
			throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"channel-desc\" : [{\"input1\" : {\"x\":\"y\"}}],\"name\" : \"source-name\",\"description\" : \"source description\",\"data-handlers\": []}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		sinkConfigReader.readSinkConfig(actualObj);
	}

	@Test
	public void testReadSinkWithHandlers() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"channel-desc\" : [\"channel1\"],\"name\" : \"source-name\",\"description\" : \"source description\",\"data-handlers\": [{     \"name\": \"kafka-data-reader\",     \"description\": \"read data from partitions specified with src-desc field\",     \"handler-class\": \"io.bigdime.core.handler.DummyConcreteHandler\",     \"properties\": {         \"broker-hosts\": \"kafka.provider.one:9092,kafka.provider.two:9096\",         \"offset-data-dir\": \"/tmp\",         \"message-size\" : \"20000\"}}]}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		SinkConfig sinkConfig = sinkConfigReader.readSinkConfig(actualObj);
		Assert.assertEquals(sinkConfig.getHandlerConfigs().size(), 1);
	}
}
