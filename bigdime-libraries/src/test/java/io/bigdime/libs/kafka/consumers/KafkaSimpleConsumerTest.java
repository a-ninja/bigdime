/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.consumers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import io.bigdime.libs.kafka.exceptions.KafkaReaderException;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * 
 * @author mnamburi
 *
 */
public class KafkaSimpleConsumerTest {

	KafkaSimpleConsumer kfkSimpleConsumer;

	@BeforeTest
	public void beforeTest() throws Exception {
		System.setProperty("env", "dev");
		List<String> brokers = Arrays.asList("kafka.provider.one:9092", "kafka.provider.two:9092");

		Properties props = new Properties();
		props.put("retentionpolicy", "1");

		kfkSimpleConsumer = KafkaSimpleConsumer.getInstance().brokers(brokers).clientId("clientID").topic("topic1")
				.partitionId("0").messageSize("100000").offSetDataDir("/tmp").properties(props);

	}

	@Test(expectedExceptions = KafkaReaderException.class)
	public void testPollDatahasError() throws KafkaReaderException, IOException {
		SimpleConsumer simpleConsumer = mock(SimpleConsumer.class);
		FetchResponse fetchResponse = mock(FetchResponse.class);
		OffsetResponse offSetResponse = mock(OffsetResponse.class);
		when(simpleConsumer.fetch(any(FetchRequest.class))).thenReturn(fetchResponse);
		
		ReflectionTestUtils.setField(kfkSimpleConsumer, "consumer", simpleConsumer);
		
		when(fetchResponse.hasError()).thenReturn(true);
		when(simpleConsumer.getOffsetsBefore(any(kafka.javaapi.OffsetRequest.class))).thenReturn(offSetResponse);
		when(offSetResponse.hasError()).thenReturn(true).thenReturn(false);
		kfkSimpleConsumer.pollData(kfkSimpleConsumer.getEarliestOffSet());
	}

	@Test
	public void testPollData() throws KafkaReaderException, IOException {
		SimpleConsumer simpleConsumer = mock(SimpleConsumer.class);
		FetchResponse fetchResponse = mock(FetchResponse.class);

		MessageAndOffset messageOffset = mock(MessageAndOffset.class, RETURNS_DEEP_STUBS);

		ByteBufferMessageSet byteBufferMessageSet = mock(ByteBufferMessageSet.class);

		Iterator<MessageAndOffset> iterator = mock(Iterator.class);
		ByteBuffer byteBuffer = ByteBuffer.wrap("HelloWorld".getBytes(Charset.defaultCharset()));// ByteBuffer.allocate(10);

		ReflectionTestUtils.setField(kfkSimpleConsumer, "consumer", simpleConsumer);

		when(simpleConsumer.fetch(any(FetchRequest.class))).thenReturn(fetchResponse);
		when(fetchResponse.hasError()).thenReturn(false);
		when(fetchResponse.messageSet(anyString(), anyInt())).thenReturn(byteBufferMessageSet);
		when(byteBufferMessageSet.iterator()).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true, false);
		when(iterator.next()).thenReturn(messageOffset);
		when(messageOffset.offset()).thenReturn( Long.valueOf(1) );
		when(messageOffset.message().payload()).thenReturn(byteBuffer);

		List<KafkaMessage> kafkaMessages = kfkSimpleConsumer.pollData(0);
		Assert.assertEquals(kafkaMessages.get(0).getMessage(), "HelloWorld".getBytes());

	}

	@Test(expectedExceptions=KafkaReaderException.class)
	public void testgetLastOffset() throws KafkaReaderException {
		long[] sampleValue = { 3l };
		SimpleConsumer simpleConsumer = mock(SimpleConsumer.class);
		OffsetResponse offsetResponse = mock(OffsetResponse.class);
		when(simpleConsumer.getOffsetsBefore(any(kafka.javaapi.OffsetRequest.class))).thenReturn(offsetResponse);
		when(offsetResponse.hasError()).thenReturn(false).thenReturn(true);
		when(offsetResponse.offsets(anyString(), anyInt())).thenReturn(sampleValue);
		ReflectionTestUtils.setField(kfkSimpleConsumer, "consumer", simpleConsumer);
		long threeOffset = kfkSimpleConsumer.getLastOffset();
		Assert.assertEquals(threeOffset, 3);
		threeOffset = kfkSimpleConsumer.getLastOffset();
		Assert.assertEquals(threeOffset, 3);
	}

//	// TODO : need to check..
//	// @Test
//	public void testStart() throws IOException {
//		Files files = mock(Files.class);
//		when(Files.exists(any(Path.class))).thenReturn(false, false);
//		when(Files.readAllBytes(any(Path.class))).thenReturn("currentOffset:10".getBytes());
//		kfkSimpleConsumer.start();
//		long offset = kfkSimpleConsumer.getCurrentOffset();
//		Assert.assertEquals(offset, 10);
//		when(Files.exists(any(Path.class))).thenReturn(false, true);
//		offset = kfkSimpleConsumer.getCurrentOffset();
//		Assert.assertEquals(offset, 0);
//	}

	@Test
	public void testStop() throws IOException {
		SimpleConsumer simpleConsumer = Mockito.mock(SimpleConsumer.class);
		Path path = FileSystems.getDefault().getPath(File.createTempFile("unit", "test").getAbsolutePath());
		ReflectionTestUtils.setField(kfkSimpleConsumer, "file", path);
		ReflectionTestUtils.setField(kfkSimpleConsumer, "consumer", simpleConsumer);
		kfkSimpleConsumer.stop();
		Mockito.verify(simpleConsumer, Mockito.times(1)).close();
	}

}
