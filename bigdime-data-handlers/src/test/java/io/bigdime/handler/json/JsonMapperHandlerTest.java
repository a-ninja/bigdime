/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.MapDescriptorParser;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.handler.constants.KafkaReaderHandlerConstants;


public class JsonMapperHandlerTest {

	private static final String DF = "yyyyMMdd";
	private static final String HOUR_FORMAT = "HH";
	private static final DateTimeZone timeZone = DateTimeZone.forID("UTC");
	private static final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern(HOUR_FORMAT).withZone(timeZone);
	private static final DateTimeFormatter formatter = DateTimeFormat.forPattern(DF).withZone(timeZone);
	
	public Map<String, Object> mockProperties() throws JsonProcessingException, IOException{
		Map<String, Object> propertyMap = new HashMap<>();
		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"topic1\",\"partition\" : \"1\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));


		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		propertyMap.put(KafkaReaderHandlerConstants.BROKERS, "unit-test-ip-1:9999,unit-test-ip-2:8888");
		propertyMap.put(KafkaReaderHandlerConstants.OFFSET_DATA_DIR, "/tmp");
		return propertyMap;
	}
	@Test
	public void testBuild() throws HandlerException, AdaptorConfigurationException, JsonProcessingException, IOException {
		
		JsonMapperHandler handler = new JsonMapperHandler();
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(handler, "runtimeInfoStore", runtimeInfoStore);
		handler.setPropertyMap(mockProperties());
		handler.build();
		Assert.assertEquals(handler.getInputDescriptor().getTopic(), "topic1");
		Assert.assertEquals(handler.getInputDescriptor().getPartition(), 1);
		Assert.assertEquals(handler.getInputDescriptor().getEntityName(), "unit-entity-name-value");

	}

	private JsonMapperHandler buildjsonMapperHandler() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		JsonMapperHandler jsonMapperHandler = new JsonMapperHandler();
		jsonMapperHandler.setPropertyMap(mockProperties());
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(jsonMapperHandler, "runtimeInfoStore", runtimeInfoStore);
		
		jsonMapperHandler.build();
		return jsonMapperHandler;
	}

	@Test
	public void testProcess() throws AdaptorConfigurationException, IOException, HandlerException {
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		byte[] jsonMessage = buildSampleJsonMessage();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(jsonMessage);
		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.createSingleItemEventList(actionEvent);
		jsonMapperHandler.process();
		HandlerJournal journal = (HandlerJournal) handlerContext.getJournal(jsonMapperHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
	}

	@Test
	public void testProcessWithChannel() throws AdaptorConfigurationException, IOException, HandlerException {
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		byte[] jsonMessage = buildSampleJsonMessage();
		DataChannel dataChannel = Mockito.mock(DataChannel.class);
		Mockito.doNothing().when(dataChannel).put(Mockito.any(ActionEvent.class));
		ReflectionTestUtils.setField(jsonMapperHandler, "outputChannel", dataChannel);
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(jsonMessage);
		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.createSingleItemEventList(actionEvent);
		jsonMapperHandler.process();
		HandlerJournal journal = (HandlerJournal) handlerContext.getJournal(jsonMapperHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
		Mockito.verify(dataChannel, Mockito.times(1)).put(actionEvent);
	}

	@Test
	public void testVerifyTheProcessFolw() throws AdaptorConfigurationException, IOException, HandlerException {
		JsonHelper jsonHelper = new JsonHelper();
		HandlerContext handlerContext = HandlerContext.get();
		ActionEvent actionEvent = new ActionEvent();
		List<ActionEvent> eventList = new ArrayList<>();
		long currentTime = System.currentTimeMillis();
		HandlerJournal journal = null;
		JsonMapperHandler jsonMapperHandler = new JsonMapperHandler();
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(jsonMapperHandler, "runtimeInfoStore", runtimeInfoStore);
		ReflectionTestUtils.setField(jsonMapperHandler, "jsonHelper", jsonHelper);
		
		Map<String, Object> properties = mockProperties();
		properties.put("timestamp", "ts");
		properties.put("partition_name", "account");
		jsonMapperHandler.setPropertyMap(properties);
		jsonMapperHandler.build();
	
		ObjectNode mockNode  = (ObjectNode) getSampleJsonNode();
		mockNode.put("ts", "2015-04-01");
		actionEvent = new ActionEvent();		
		actionEvent.setBody(mockNode.toString().getBytes());
		eventList.add(actionEvent);	
		
		mockNode.put("ts", System.currentTimeMillis());
		actionEvent = new ActionEvent();		
		actionEvent.setBody(mockNode.toString().getBytes());
		eventList.add(actionEvent);

		handlerContext.setEventList(eventList);
		jsonMapperHandler.process();
		journal = (HandlerJournal) handlerContext.getJournal(jsonMapperHandler.getId());
		Assert.assertEquals(journal.getEventList().size(), 1);
		List<ActionEvent>  eventList101 =  handlerContext.getEventList();
		ActionEvent actionEvent1= eventList101.get(0);
		Assert.assertEquals(actionEvent1.getHeaders().get("DT"), "20150401");
		Assert.assertEquals(actionEvent1.getHeaders().get("HOUR"), "00");
		
		jsonMapperHandler.process();
		eventList101 =  handlerContext.getEventList();
		actionEvent1= eventList101.get(0);
		
		DateTime dateTime = new DateTime(currentTime);
		Assert.assertEquals(actionEvent1.getHeaders().get("DT"),formatter.print(dateTime));
		Assert.assertEquals(actionEvent1.getHeaders().get("HOUR"),hourFormatter.print(dateTime));
		
		journal = (HandlerJournal) handlerContext.getJournal(jsonMapperHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
	}	
	
	private byte[] buildSampleJsonMessage() throws IOException {
		String msg = "{\"name\": \"John Doe1\", \"favorite_number\": {\"int\": 421}, \"favorite_color\":{\"string\": \"Blue\"}}";
		JsonNode m = buildJsonMessage(msg);
		byte[] jsonMessage = m.toString().getBytes();
		return jsonMessage;
	}

	
	private JsonNode getSampleJsonNode() throws IOException {
		String msg = "{\"name\": \"John Doe1\", \"favorite_number\": {\"int\": 421}, \"favorite_color\":{\"string\": \"Blue\"}}";
		JsonNode messageNode = buildJsonMessage(msg);
		return messageNode;
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

	/**
	 * Assert that id the data is there in journal, it can be picked up and if
	 * more than one records are there, CALLBACK is returned.
	 * 
	 * @throws HandlerException
	 * @throws IOException
	 * @throws AdaptorConfigurationException 
	 */
	@Test
	public void testWithDataInJournal() throws HandlerException, IOException, AdaptorConfigurationException {
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		HandlerJournal journal = new SimpleJournal();
		journal.setTotalRead(1);
		journal.setTotalSize(10);
		HandlerContext.get().setJournal(jsonMapperHandler.getId(), journal);

		List<ActionEvent> actionEvents = new ArrayList<>();
		ActionEvent actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn(buildSampleJsonMessage());
		actionEvents.add(actionEvent);
		actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn(buildSampleJsonMessage());
		actionEvents.add(Mockito.mock(ActionEvent.class));
		journal.setEventList(actionEvents);
		Status status = jsonMapperHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
	}
	
	@Test
	public void testisValidationReady() throws AdaptorConfigurationException, JsonProcessingException, IOException{
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		boolean validationNotReady = 	jsonMapperHandler.isValidationReady(null, null);
		Assert.assertFalse(validationNotReady);
		validationNotReady = 	jsonMapperHandler.isValidationReady("20160101", "00");
		Assert.assertFalse(validationNotReady);
		validationNotReady = 	jsonMapperHandler.isValidationReady("20160101", "02");
		Assert.assertTrue(validationNotReady);
		validationNotReady = 	jsonMapperHandler.isValidationReady("20160102", "02");
		Assert.assertTrue(validationNotReady);	
		validationNotReady = 	jsonMapperHandler.isValidationReady("20160102", "03");
		Assert.assertTrue(validationNotReady);		
		validationNotReady = 	jsonMapperHandler.isValidationReady("20160102", "03");
		Assert.assertFalse(validationNotReady);			
	}
}
