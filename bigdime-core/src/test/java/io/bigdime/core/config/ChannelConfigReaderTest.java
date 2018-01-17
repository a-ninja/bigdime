/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bigdime.core.commons.JsonHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@Configuration
@ContextConfiguration(classes = { JsonHelper.class, ChannelConfigReader.class })
//@ContextConfiguration({ "classpath*:META-INF/application-context-config.xml" })
public class ChannelConfigReaderTest extends AbstractTestNGSpringContextTests {

	@Autowired
	ChannelConfigReader channelConfigReader;

	@Test
	public void testInstance() {
		Assert.assertNotNull(channelConfigReader);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testGetChannelWithNoChannelClass() throws Throwable {
		try {
			String jsonString = "{\"name\" : \"unit-channel-name-1\",\"description\" : \"unit-channel-description\"}";
			ObjectMapper mapper = new ObjectMapper();
			JsonNode actualObj = mapper.readTree(jsonString);

			channelConfigReader.readChannelConfig(actualObj);
		} catch (Exception ex) {
			throw ex.getCause();
		}

	}

}
