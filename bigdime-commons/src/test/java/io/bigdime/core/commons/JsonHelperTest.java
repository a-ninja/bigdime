/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@Configuration
@ContextConfiguration({ "classpath*:META-INF/application-context-json-helper.xml" })
public class JsonHelperTest extends AbstractTestNGSpringContextTests {

	@Autowired
	private JsonHelper jsonHelper;

	/**
	 * Make sure that if a required textual node is present but is not actually
	 * a text node, an IllegalArgumentException is thrown.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "no text node found with key=notTextualRequiredNodeKey")
	public void testGetRequiredStringPropertyWithNotTextualNode() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String notTextualRequiredNodeKey = "notTextualRequiredNodeKey";
		JsonNode notTextualRequiredNode = Mockito.mock(JsonNode.class);
		Mockito.when(node.get(notTextualRequiredNodeKey)).thenReturn(notTextualRequiredNode);
		Mockito.when(notTextualRequiredNode.isTextual()).thenReturn(false);
		jsonHelper.getRequiredStringProperty(node, notTextualRequiredNodeKey);
		Mockito.verify(node, Mockito.times(1)).get(notTextualRequiredNodeKey);
	}

	/**
	 * Make sure that if a required textual node is present but is actually a
	 * text node, valid value is returned.
	 */
	@Test
	public void testGetRequiredStringProperty() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String textualRequiredNodeKey = "textualRequiredNodeKey";
		JsonNode textualRequiredNode = Mockito.mock(JsonNode.class);
		Mockito.when(node.get(textualRequiredNodeKey)).thenReturn(textualRequiredNode);
		Mockito.when(textualRequiredNode.isTextual()).thenReturn(true);
		Mockito.when(textualRequiredNode.getTextValue()).thenReturn("textualRequiredNodeValue");
		String propertyValue = jsonHelper.getRequiredStringProperty(node, textualRequiredNodeKey);

		Assert.assertEquals(propertyValue, "textualRequiredNodeValue");
		Mockito.verify(node, Mockito.times(1)).get(textualRequiredNodeKey);
	}

	/**
	 * If the required node is unavailable, IllegalArgumentException should be
	 * thrown.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "no node found with key=requiredNode")
	public void testGetRequiredNodeWithUnavailableNode() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String key = "requiredNode";
		jsonHelper.getRequiredNode(node, key);
		Mockito.verify(node, Mockito.times(1)).get(key);
	}

	/**
	 * If the required node is present, it should be returned.
	 */

	@Test
	public void testGetRequiredNode() {
		JsonNode node = Mockito.mock(JsonNode.class);
		JsonNode requiredNode = Mockito.mock(JsonNode.class);
		String key = "requiredNode";
		Mockito.when(node.get(key)).thenReturn(requiredNode);
		JsonNode actualNode = jsonHelper.getRequiredNode(node, key);
		Assert.assertEquals(actualNode, requiredNode);
		Mockito.verify(node, Mockito.times(1)).get(key);
	}

	/**
	 * If the required node is present and is array, it should be returned.
	 */
	@Test
	public void testGetRequiredArrayNode() {
		JsonNode node = Mockito.mock(JsonNode.class);
		JsonNode requiredArrayNode = Mockito.mock(JsonNode.class);
		String requiredNodeKey = "requiredArrayNode";
		Mockito.when(node.get(requiredNodeKey)).thenReturn(requiredArrayNode);
		Mockito.when(requiredArrayNode.isArray()).thenReturn(true);
		JsonNode actualNode = jsonHelper.getRequiredArrayNode(node, requiredNodeKey);
		Assert.assertEquals(actualNode, requiredArrayNode);
		Mockito.verify(node, Mockito.times(1)).get(requiredNodeKey);
	}

	/**
	 * If the required node is present and is NOT array,
	 * IllegalArgumentException should be thrown.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "no array node found with key=requiredArrayNode")
	public void testGetRequiredArrayNodeWithNotArrayNode() {
		JsonNode node = Mockito.mock(JsonNode.class);
		JsonNode requiredArrayNode = Mockito.mock(JsonNode.class);
		String requiredNodeKey = "requiredArrayNode";
		Mockito.when(node.get(requiredNodeKey)).thenReturn(requiredArrayNode);
		Mockito.when(requiredArrayNode.isArray()).thenReturn(false);
		JsonNode actualNode = jsonHelper.getRequiredArrayNode(node, requiredNodeKey);
		Assert.assertEquals(actualNode, requiredArrayNode);
		Mockito.verify(node, Mockito.times(1)).get(requiredNodeKey);
	}

	@Test
	public void testGetNodeTree() throws JsonProcessingException, IOException {

		String jsonString = "\"properties\": {\"channel-map\" : \"input1:c1, input2:c2, input3:c3\",\"arrayKey\" : [\"array1\", \"array2\"],\"arrayNodeKey\" : [{\"name\" : \"nameValueArrayNode1\", \"id\" : \"idValueArrayNode1\"},{\"name\" : \"nameValueArrayNode2\", \"id\" : \"idValueArrayNode2\"}],\"nodeKey\" : {\"name\" : \"nameValueNode\",\"address\" : \"addressValueNode\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);

		jsonHelper.getNodeTree(actualObj);
	}

	@Test
	public void testGetNodeTreeNode() {
		JsonNode node = Mockito.mock(JsonNode.class);
		@SuppressWarnings("unchecked")
		Iterator<Entry<String, JsonNode>> mockIter = Mockito.mock(Iterator.class);
		Mockito.when(mockIter.hasNext()).thenReturn(true).thenReturn(false);

		Mockito.when(node.getFields()).thenReturn(mockIter);

		@SuppressWarnings("unchecked")
		Entry<String, JsonNode> mockEntry = Mockito.mock(Entry.class);
		Mockito.when(mockIter.next()).thenReturn(mockEntry);
		JsonNode nodeNode = Mockito.mock(JsonNode.class);
		Mockito.when(mockEntry.getValue()).thenReturn(nodeNode);
		jsonHelper.getNodeTree(node);
	}

	/**
	 * Make sure that if a boolean is present and has value as "true", true is
	 * returned.
	 */
	@Test
	public void testGetBooleanPropertyWithNodeHasTrue() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String booleanNodeKey = "booleanNodeKey";
		JsonNode booleanNode = Mockito.mock(JsonNode.class);
		Mockito.when(node.get(booleanNodeKey)).thenReturn(booleanNode);
		Mockito.when(booleanNode.isTextual()).thenReturn(true);
		Mockito.when(booleanNode.getTextValue()).thenReturn("true");
		boolean propertyValue = jsonHelper.getBooleanProperty(node, booleanNodeKey);

		Assert.assertTrue(propertyValue);
		Mockito.verify(node, Mockito.times(1)).get(booleanNodeKey);
	}

	/**
	 * Make sure that if a boolean is present and has value as "false", false is
	 * returned.
	 */
	@Test
	public void testGetBooleanPropertyWithNodeHasFalse() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String booleanNodeKey = "booleanNodeKey";
		JsonNode booleanNode = Mockito.mock(JsonNode.class);
		Mockito.when(node.get(booleanNodeKey)).thenReturn(booleanNode);
		Mockito.when(booleanNode.isTextual()).thenReturn(true);
		Mockito.when(booleanNode.getTextValue()).thenReturn("false");
		boolean propertyValue = jsonHelper.getBooleanProperty(node, booleanNodeKey);
		Assert.assertFalse(propertyValue);
		Mockito.verify(node, Mockito.times(1)).get(booleanNodeKey);
	}

	/**
	 * Make sure that if a boolean is present and has value is neither true nor
	 * false, false is returned.
	 */
	@Test
	public void testGetBooleanPropertyWithNodeHasAnyStringValue() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String booleanNodeKey = "booleanNodeKey";
		JsonNode booleanNode = Mockito.mock(JsonNode.class);
		Mockito.when(node.get(booleanNodeKey)).thenReturn(booleanNode);
		Mockito.when(booleanNode.isTextual()).thenReturn(true);
		Mockito.when(booleanNode.getTextValue()).thenReturn("unit-string-1");
		boolean propertyValue = jsonHelper.getBooleanProperty(node, booleanNodeKey);
		Assert.assertFalse(propertyValue);
		Mockito.verify(node, Mockito.times(1)).get(booleanNodeKey);
	}

	/**
	 * Make sure that if the node is not present, false is returned.
	 */
	@Test
	public void testGetBooleanPropertyWithNodeNotPresent() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String booleanNodeKey = "booleanNodeKey";
		boolean propertyValue = jsonHelper.getBooleanProperty(node, booleanNodeKey);

		Assert.assertFalse(propertyValue);
		Mockito.verify(node, Mockito.times(1)).get(booleanNodeKey);
	}

	@Test
	public void testGetNodeTree1() throws JsonProcessingException, IOException {
		String jsonString = "{\"channel-map\" : \"input1:c1, input2:c2, input3:c3\",\"arrayKey\" : [\"array1\", \"array2\"],\"arrayNodeKey\" : [{\"name\" : \"nameValueArrayNode1\", \"id\" : \"idValueArrayNode1\"},{\"name\" : \"nameValueArrayNode2\", \"id\" : \"idValueArrayNode2\"}],\"nodeKey\" : {\"name\" : \"nameValueNode\",\"address\" : \"addressValueNode\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);

		Map<String, Object> properties = jsonHelper.getNodeTree(actualObj);
		Assert.assertEquals(properties.get("channel-map"), "input1:c1, input2:c2, input3:c3");
		System.out.println(properties.get("arrayKey"));
		Assert.assertTrue(properties.get("arrayKey") instanceof ArrayList);
		Assert.assertTrue(((ArrayList<?>) properties.get("arrayKey")).contains("array1"));
		Assert.assertTrue(((ArrayList<?>) properties.get("arrayKey")).contains("array2"));
	}
	/**
	 *  if a required key is not present in the node,  an IllegalArgumentException is thrown.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "no node found with key=notTextualRequiredNodeKey")
	public void testGetRequiredProperty() {
		JsonNode node = Mockito.mock(JsonNode.class);
		String notTextualRequiredNodeKey = "notTextualRequiredNodeKey";
		Mockito.when(node.get(notTextualRequiredNodeKey)).thenReturn(null);
		jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Mockito.verify(node, Mockito.times(1)).get(notTextualRequiredNodeKey);
	}
	/**
	 * Make sure that if a required key is  present in the node,  an IllegalArgumentException is thrown.
	 */
	@Test
	public void testGetRequiredPropertyIfPresent() {
		Long nodeLongValue = 100l;
		Boolean nodeBooleanValue = true;
		int nodeNumberValue = 111;
		double nodeDoubleValue = 11.101;
		String nodeStringValue = "testnodeStringValue";
		
		JsonNode node = Mockito.mock(JsonNode.class);
		String notTextualRequiredNodeKey = "notTextualRequiredNodeKey";
		JsonNode notTextualRequiredNode = Mockito.mock(JsonNode.class);
		Mockito.when(notTextualRequiredNode.isBoolean()).thenReturn(true).thenReturn(false);
		Mockito.when(notTextualRequiredNode.isNumber()).thenReturn(true).thenReturn(false);
		Mockito.when(notTextualRequiredNode.isLong()).thenReturn(true).thenReturn(false);
		Mockito.when(notTextualRequiredNode.isDouble()).thenReturn(true).thenReturn(false);
		Mockito.when(notTextualRequiredNode.isTextual()).thenReturn(true).thenReturn(false);

		Mockito.when(notTextualRequiredNode.getTextValue()).thenReturn(nodeStringValue);
		Mockito.when(notTextualRequiredNode.getLongValue()).thenReturn(nodeLongValue);
		Mockito.when(notTextualRequiredNode.getBooleanValue()).thenReturn(nodeBooleanValue);
		Mockito.when(notTextualRequiredNode.getNumberValue()).thenReturn(nodeNumberValue);
		Mockito.when(notTextualRequiredNode.getDoubleValue()).thenReturn(nodeDoubleValue);
		Mockito.when(notTextualRequiredNode.asText()).thenReturn(nodeStringValue);

		Mockito.when(node.get(notTextualRequiredNodeKey)).thenReturn(notTextualRequiredNode);
		
		Object object = jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Assert.assertEquals(object, nodeBooleanValue);
		object = jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Assert.assertEquals(object, nodeNumberValue);
		object = jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Assert.assertEquals(object, nodeLongValue);
		object = jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Assert.assertEquals(object, nodeDoubleValue);
		object = jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Assert.assertEquals(object, nodeStringValue);
		object = jsonHelper.getRequiredProperty(node, notTextualRequiredNodeKey);
		Assert.assertEquals(object, nodeStringValue);		
		Mockito.verify(node, Mockito.times(6)).get(notTextualRequiredNodeKey);
	}	
	
	
	@Test
	public void testFindJsonNode() throws JsonProcessingException, IOException{
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"topic1\",\"partition\" : \"1\", \"unit-separator\" : \"unit-separator-value\"}}";
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode node = objectMapper.readTree(jsonString.getBytes());

		ObjectNode objectNode = jsonHelper.find(node, "partition1");
		Assert.assertNull(objectNode);

		objectNode = jsonHelper.find(node, "topic");
		Object nodeValue = jsonHelper.getRequiredProperty(objectNode, "topic");
		Assert.assertEquals(nodeValue, "topic1");

		objectNode = jsonHelper.find(node, "partition");
		nodeValue = jsonHelper.getRequiredProperty(objectNode, "partition");
		Assert.assertEquals(nodeValue,"1");
	}
}
