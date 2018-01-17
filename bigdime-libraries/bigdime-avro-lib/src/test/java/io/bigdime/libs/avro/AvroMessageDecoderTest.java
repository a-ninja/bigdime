/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public class AvroMessageDecoderTest {
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testDecodeWithNullSchemaFile() {
		new AvroMessageEncoderDecoder(null);
	}

	@Test(expectedExceptions = FileNotFoundException.class)
	public void testDecodeWithBadSchemaFile() throws Throwable {
		try {
			new AvroMessageEncoderDecoder("unit-schemaFileName", 0);
		} catch (Exception ex) {
			throw ex.getCause();
		}
	}

	@Test
	public void testDecode() throws IOException {
		AvroMessageEncoderDecoder avroMessageDecoder = new AvroMessageEncoderDecoder("avro-schema-file.avsc");

		String msg = "{\"name\": \"John Doe\", \"favorite_number\": {\"int\": 421}, \"favorite_color\":{\"string\": \"Blue\"}}";
		JsonNode m = buildJsonMessage(msg);
		byte[] avroMsg = avroMessageDecoder.encode(m);
		JsonNode jn = avroMessageDecoder.decode(avroMsg);
		Iterator<Entry<String, JsonNode>> entryIterator = jn.fields();
		Entry<String, JsonNode> entry = entryIterator.next();

		Assert.assertEquals(entry.getKey(), "name");
		Assert.assertEquals(entry.getValue().asText(), "John Doe");

		entry = entryIterator.next();
		Assert.assertEquals(entry.getKey(), "favorite_number");
		Assert.assertEquals(entry.getValue().asInt(), 421);

		entry = entryIterator.next();
		Assert.assertEquals(entry.getKey(), "favorite_color");
		Assert.assertEquals(entry.getValue().asText(), "Blue");
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
