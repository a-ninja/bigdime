/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.utills;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 * @author mnamburi
 * Utils class to support Kafka Helper functions.
 */
public class KafkaUtilsTests {

	
	@Test
	public void testParseBrokers(){
		String mockBrokers = "kafka.provider.one:9092,kafka.provider.two:9096";
		List<String> list = KafkaUtils.parseBrokers(mockBrokers, null);
		Assert.assertEquals(2, list.size());
		mockBrokers = "kafka.provider.one:9092&&kafka.provider.two:90962&&kafka.provider.three:9096";
		list = KafkaUtils.parseBrokers(mockBrokers, "&&");
		Assert.assertEquals(3, list.size());

	}

}
