/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hive;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.MapDescriptorParser;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.handler.constants.KafkaReaderHandlerConstants;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@ContextConfiguration(classes = { MetaDataJsonUtils.class})
public class AdaptorMetaDataHandlerTest extends AbstractTestNGSpringContextTests{
	
	@Mock
	MetadataStore metadataStore;
	@Autowired
	MetaDataJsonUtils metaDataJsonUtils;
	ObjectMapper objectMapper1 = new ObjectMapper();
	String trackingSchema = null;
	Properties props = new Properties();
	
	@BeforeTest
	public void loadSchemaFile() throws IOException{
		
		File resourcesDir = new File (System.getProperty("user.dir") + "/src/test/resources/click-stream-hive-schema.json");
		trackingSchema = FileUtils.readFileToString(resourcesDir);
	}
	
	
	public Map<String, Object> mockProperties() throws JsonProcessingException, IOException{
		Map<String, Object> propertyMap = new HashMap<>();
		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"topic1\",\"partition\" : \"1\", \"schemaFileName\" : \"click-stream-hive-schema.json\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));


		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		propertyMap.put(KafkaReaderHandlerConstants.BROKERS, "unit-test-ip-1:9999,unit-test-ip-2:8888");
		return propertyMap;
	}

	@Test
	public void testProcess() throws ClientProtocolException, IOException, InterruptedException, MetadataAccessException, HandlerException{
		AdaptorMetaDataHandler adapterMetaDataHandler = new AdaptorMetaDataHandler();

		HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = new ArrayList<>();
		ActionEvent actionEvent = new ActionEvent();
		HashMap<String,String> headers = new HashMap<String,String>();
		headers.put(ActionEventHeaderConstants.ENTITY_NAME, "clickStreamEvents");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "account,dt");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "testaccount,20150101");
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_LOCATION, FileUtils.getTempDirectoryPath()+File.separator
				+headers.get(ActionEventHeaderConstants.ENTITY_NAME)+File.separator+"20150101");

		actionEvent.setHeaders(headers);
		actionEvents.add(actionEvent);
		handlerContext.setEventList(actionEvents);
		Status status = adapterMetaDataHandler.process();
		Assert.assertEquals(Status.READY, status);
	}

	@Test
	public void testBuild() throws JsonProcessingException, IOException, AdaptorConfigurationException, InterruptedException, MetadataAccessException{
		AdaptorMetaDataHandler adapterMetaDataHandler = mockAdapterMetaDataHandler();
		adapterMetaDataHandler.setPropertyMap(mockProperties());
		adapterMetaDataHandler.build();
		adapterMetaDataHandler.build();
	}

	private AdaptorMetaDataHandler mockAdapterMetaDataHandler()
			throws ClientProtocolException, IOException, InterruptedException, MetadataAccessException {
		AdaptorMetaDataHandler hiveMetaDataHandler = new AdaptorMetaDataHandler();
		metadataStore = Mockito.mock(MetadataStore.class);
		Metasegment metaSegment = metaDataJsonUtils.convertJsonToMetaData("mock-app","clickStreamEvents",
				objectMapper1.readTree(trackingSchema.getBytes()));
		when(metadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(null).thenReturn(metaSegment);
		when(metadataStore.getAdaptorEntity(anyString(), anyString(), anyString())).thenReturn(metaSegment.getEntity("clickStreamEvents"));
		metaSegment.setDatabaseLocation(FileUtils.getUserDirectoryPath() +File.separator+"BD_TEST"+"_"+System.currentTimeMillis()+metaSegment.getDatabaseName()+File.separator);
		ReflectionTestUtils.setField(hiveMetaDataHandler, "metadataStore", metadataStore);
		ReflectionTestUtils.setField(hiveMetaDataHandler, "metaDataJsonUtils", metaDataJsonUtils);
		return hiveMetaDataHandler;
	}	
}
