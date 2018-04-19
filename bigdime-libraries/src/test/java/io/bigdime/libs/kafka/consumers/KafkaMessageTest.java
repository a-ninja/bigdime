/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.consumers;

import org.testng.Assert;
import org.testng.annotations.Test;

import scala.util.Random;

public class KafkaMessageTest {

	@Test
	public void testGettersAndSetters() {
		KafkaMessage kafkaMessage = new KafkaMessage();
		kafkaMessage.setMessage("unit-testGettersAndSetters-1".getBytes());
		Random rand = new Random();
		long offset = rand.nextLong();
		kafkaMessage.setOffset(offset);
		int partiton = rand.nextInt(10);
		kafkaMessage.setPartition(partiton);
		kafkaMessage.setTopicName("unit-testGettersAndSetters-topic-name-1");

		Assert.assertEquals(new String(kafkaMessage.getMessage()), "unit-testGettersAndSetters-1");
		Assert.assertEquals(kafkaMessage.getOffset(), offset);
		Assert.assertEquals(kafkaMessage.getPartition(), partiton);
		Assert.assertEquals(kafkaMessage.getTopicName(), "unit-testGettersAndSetters-topic-name-1");
	}
}
