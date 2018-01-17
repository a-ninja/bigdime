/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.kafka.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.Sink;
import io.bigdime.core.adaptor.DataAdaptor;
import io.bigdime.handler.kafka.KafkaReaderHandler;
import io.bigdime.libs.avro.AvroMessageEncoderDecoder;
import io.bigdime.libs.kafka.consumers.KafkaMessage;
import io.bigdime.libs.kafka.consumers.KafkaSimpleConsumer;
import io.bigdime.libs.kafka.exceptions.KafkaReaderException;
import io.bigdime.testng.BasicTest;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Test(singleThreaded = true)
public class KafkaReaderHandlerIntegrationTest extends BasicTest {

	@Autowired
	DataAdaptor adaptor;

	/**
	 * 
	 * @throws DataAdaptorException
	 * @throws KafkaReaderException
	 * @throws HandlerException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test(groups = "KafkaReaderHandlerIntegrationTest-1", singleThreaded = true)
	public void testStart()
			throws DataAdaptorException, KafkaReaderException, HandlerException, IOException, InterruptedException {
		// We know that KafkaReaderHandler is the first handler in the chain, so
		// get it from the Source.
		KafkaReaderHandler kafkaReaderHandler = (KafkaReaderHandler) adaptor.getSources().iterator().next()
				.getHandlers().iterator().next();
		// Setup KafkaReaderHandler now.
		KafkaSimpleConsumer kafkaSimpleConsumer = Mockito.mock(KafkaSimpleConsumer.class);
		List<KafkaMessage> kafkaMessages = new ArrayList<>();
		KafkaMessage kafkaMessage = Mockito.mock(KafkaMessage.class);

		String msg = "{\"name\": \"John Doe\", \"favorite_number\": {\"int\": 421}, \"favorite_color\":{\"string\": \"Blue\"}}";
		JsonNode m = buildJsonMessage(msg);
		AvroMessageEncoderDecoder avroMessageDecoder = new AvroMessageEncoderDecoder("avro-schema-file.avsc");
		byte[] avroMsg = avroMessageDecoder.encode(m);

		// with 2 messages on list, first two loops on KafkaReaderHandler will
		// process the 2 msgs. third call will get null from pollData and hence
		// it'll return a status to BACKOFF.
		Mockito.when(kafkaMessage.getMessage()).thenReturn(avroMsg);
		kafkaMessages.add(kafkaMessage);
		kafkaMessages.add(kafkaMessage);// add two messages to the list.
		Mockito.when(kafkaSimpleConsumer.pollData(kafkaSimpleConsumer.getEarliestOffSet())).thenReturn(kafkaMessages).thenReturn(null);
		ReflectionTestUtils.setField(kafkaReaderHandler, "kafkaSimpleConsumer", kafkaSimpleConsumer);
		// now start the adaptor.
		for (Sink sink : adaptor.getSinks()) {
			ReflectionTestUtils.setField(sink, "sleepForMillis", 100);
		}
		adaptor.start();
		Thread.sleep(1000);
		Assert.assertTrue(kafkaReaderHandler.getTotalInvocations() > 1);
		adaptor.stop();
	}

	public static JsonNode buildJsonMessage(String str) {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = null;
		try {
			actualObj = mapper.readTree(str);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		return actualObj;
	}

}
