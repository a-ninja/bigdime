/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.libs.avro.AvroMessageEncoderDecoder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class AvroRecordHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		buildAvroRecordHandler();
	}

	private AvroJsonMapperHandler buildAvroRecordHandler() throws AdaptorConfigurationException {
		AvroJsonMapperHandler avroRecordHandler = new AvroJsonMapperHandler();
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("schemaFileName", "avro-schema-file.avsc");
		avroRecordHandler.setPropertyMap(propertyMap);
		avroRecordHandler.build();
		return avroRecordHandler;
	}

	@Test
	public void testProcess() throws AdaptorConfigurationException, IOException, HandlerException {
		AvroJsonMapperHandler avroRecordHandler = buildAvroRecordHandler();
		byte[] avroMsg = buildSampleAvroMessage();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(avroMsg);
		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.createSingleItemEventList(actionEvent);
		avroRecordHandler.process();
		HandlerJournal journal = (HandlerJournal) handlerContext.getJournal(avroRecordHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
	}

	@Test
	public void testProcessWithChannel() throws AdaptorConfigurationException, IOException, HandlerException {
		AvroJsonMapperHandler avroRecordHandler = buildAvroRecordHandler();
		byte[] avroMsg = buildSampleAvroMessage();
		DataChannel dataChannel = Mockito.mock(DataChannel.class);
		Mockito.doNothing().when(dataChannel).put(Mockito.any(ActionEvent.class));
		ReflectionTestUtils.setField(avroRecordHandler, "outputChannel", dataChannel);
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(avroMsg);
		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.createSingleItemEventList(actionEvent);
		avroRecordHandler.process();
		HandlerJournal journal = (HandlerJournal) handlerContext.getJournal(avroRecordHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
		Mockito.verify(dataChannel, Mockito.times(1)).put(actionEvent);
	}

	private byte[] buildSampleAvroMessage() throws IOException {
		AvroMessageEncoderDecoder avroMessageDecoder = new AvroMessageEncoderDecoder("avro-schema-file.avsc");

		String msg = "{\"name\": \"John Doe1\", \"favorite_number\": {\"int\": 421}, \"favorite_color\":{\"string\": \"Blue\"}}";
		JsonNode m = buildJsonMessage(msg);
		byte[] avroMsg = avroMessageDecoder.encode(m);
		return avroMsg;
	}

	private byte[] buildSampleClickStreamAvroMessage(long part) throws IOException {
		TrackingEvents trackingEvents = new TrackingEvents();
		trackingEvents.setAccount("mweb-us");
		Map<CharSequence, CharSequence> contextMap = new HashMap<CharSequence, CharSequence>();
		contextMap.put("browserHeight", "927");
		trackingEvents.setContext(contextMap);

		List<TrackingEvent> events = new ArrayList<TrackingEvent>();
		TrackingEvent event = new TrackingEvent();
		event.setName("core:misc");
		event.setTimestamp(new Date().toString());
		Map<CharSequence, CharSequence> propertiesMap = new HashMap<CharSequence, CharSequence>();
		propertiesMap.put("account", "us-mweb");
		propertiesMap.put("serverTimestamp", "2015-05-01T04:45:17Z");

		propertiesMap.put("cookiesEnabled", "Y");
		propertiesMap.put("unit-test", "true");
		propertiesMap.put("part", "part-" + part);

		event.setProperties(propertiesMap);
		events.add(event);
		trackingEvents.setEvents(events);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<TrackingEvents> writer = new SpecificDatumWriter<TrackingEvents>(TrackingEvents.getClassSchema());

		writer.write(trackingEvents, encoder);
		encoder.flush();
		out.close();
		byte[] serializedBytes = out.toByteArray();
		return serializedBytes;
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

	// @Test
	public void kafkaProducer() throws IOException {
		Properties props = new Properties();
		props.put("metadata.broker.list",
				"kafka1:9092,kafka2:9092,kafka3:9092");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("partitioner.class", "io.bigdime.libs.kafka.partitioners.BytePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<byte[], byte[]> producer = new Producer<>(config);

		for (long nEvents = 0; nEvents < 6; nEvents++) {
			KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>("tracking_events",
					("key." + nEvents).getBytes(), buildSampleClickStreamAvroMessage(nEvents));
			producer.send(data);
		}
		producer.close();
	}

	/**
	 * Assert that id the data is there in journal, it can be picked up and if
	 * more than one records are there, CALLBACK is returned.
	 * 
	 * @throws HandlerException
	 * @throws IOException
	 */
	@Test
	public void testWithDataInJournal() throws HandlerException, IOException {
		AvroJsonMapperHandler avroJsonMapperHandler = new AvroJsonMapperHandler();
		HandlerJournal journal = new SimpleJournal();
		journal.setTotalRead(1);
		journal.setTotalSize(10);
		HandlerContext.get().setJournal(avroJsonMapperHandler.getId(), journal);

		List<ActionEvent> actionEvents = new ArrayList<>();
		ActionEvent actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn("unit-json".getBytes());
		actionEvents.add(actionEvent);
		actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn("unit-json".getBytes());
		actionEvents.add(Mockito.mock(ActionEvent.class));
		journal.setEventList(actionEvents);

		AvroMessageEncoderDecoder avroMessageEncoderDecoder = Mockito.mock(AvroMessageEncoderDecoder.class);
		ReflectionTestUtils.setField(avroJsonMapperHandler, "avroMessageDecoder", avroMessageEncoderDecoder);

		JsonNode jsonNode = Mockito.mock(JsonNode.class);
		Mockito.when(jsonNode.toString()).thenReturn("unit-json");
		Mockito.when(avroMessageEncoderDecoder.decode("unit-json".getBytes())).thenReturn(jsonNode);
		Status status = avroJsonMapperHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
	}

}
