/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.integration;

import io.bigdime.common.testutils.TestUtils;
import io.bigdime.common.testutils.factory.EmbeddedKafkaServer;
import io.bigdime.common.testutils.factory.EmbeddedZookeeperServerFactory;
import io.bigdime.libs.kafka.constants.KafkaChannelConstants;
import io.bigdime.libs.kafka.consumers.KafkaMessage;
import io.bigdime.libs.kafka.consumers.KafkaSimpleConsumer;
import io.bigdime.libs.kafka.exceptions.KafkaReaderException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * 
 * @author mnamburi
 *
 */
public class KafkaSimpleConsumerTest {
	private static Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumerTest.class);

	private int zookeeperAvailbePort  = 0;
	private  int kafkaPort  = 0;

	private String serializer_class ="kafka.serializer.DefaultEncoder";
	private String partitioner_class ="io.bigdime.libs.kafka.partitioners.BytePartitioner";
	private String request_required_acks ="1";
	private List<EmbeddedKafkaServer> embeddedKafkaServers = new ArrayList<EmbeddedKafkaServer>();		
	EmbeddedZookeeperServerFactory embeddedZookeeper = null;
	EmbeddedKafkaServer embeddedKafkaServer = null;
	StringBuilder seedBrokers = new StringBuilder();

	String path = FileUtils.getTempDirectoryPath();
	String zookeeperData = path + "zookeeper/data";
	String zookeeperLog = path + "zookeeper/log";
	String kafkaLogs = path + "kafkaLogs";

//	String zookeeperData = "/tmp/zookeeper/data";
//	String zookeeperLog = "/tmp/zookeeper/log";
//	String kafkaLogs = "/tmp/kafka/logs/" + "kafkaLogs";
	private  final int zeroPort = 0;
	@BeforeTest
	public void beforeTest() throws Exception {
		System.setProperty("env", "dev");
	}	
	
	@AfterTest
	public void cleanResouces() throws Exception{
		
		for (EmbeddedKafkaServer embeddedKafkaServer : embeddedKafkaServers) {
			try {
				embeddedKafkaServer.shutdown();
				logger.info("Shut down kafka server - run status is: {} ",  embeddedKafkaServer.isActive());
			} catch (Exception e) {
				logger.info("ERROR: Embedded kafka server failed to stop: "
						+ e.getMessage(), e);
			}
		}
		embeddedZookeeper.shutdownZookeeper();
		File file = new File((path));
		if(file.exists() ) {
			FileUtils.forceDelete(file);
		}
	}
	

	@BeforeClass
	public void init() throws Exception {
		// Start the internal kafka cluster.
		// Note: this provider starts up a zookeeper instance.
		//***********************************************
		zookeeperAvailbePort = TestUtils.findAvailablePort(zeroPort);
		embeddedZookeeper  = EmbeddedZookeeperServerFactory.getInstance(zookeeperData, zookeeperLog, "kafka.provider.one", zookeeperAvailbePort);

		embeddedZookeeper.startZookeeper();
		kafkaPort = TestUtils.findAvailablePort(zeroPort);

		embeddedKafkaServer = EmbeddedKafkaServer.newServer()
		.host("kafka.provider.one")
		.port(Integer.toString(kafkaPort))
		.brokerId("0")
		.zookeeperConnection("kafka.provider.one", zookeeperAvailbePort, "kafka")
		.logDirs(kafkaLogs+"/kafkaLogs1")
		.numPartitions("2")
		.messageMaxBytes("1000000")
		.build();
		embeddedKafkaServer.startup();
		embeddedKafkaServers.add(embeddedKafkaServer);
		
		seedBrokers.append("kafka.provider.one:"+Integer.toString(kafkaPort));
		kafkaPort = TestUtils.findAvailablePort(zeroPort);

		embeddedKafkaServer = EmbeddedKafkaServer.newServer()
		.host("kafka.provider.two")
		.port(Integer.toString(kafkaPort))
		.brokerId("1")
		.zookeeperConnection("kafka.provider.one", zookeeperAvailbePort, "kafka")
		.logDirs(kafkaLogs+"/kafkaLogs2")
		.numPartitions("2")
		.messageMaxBytes("1000000")
		.build();
		embeddedKafkaServer.startup();
		seedBrokers.append(",");
		seedBrokers.append("kafka.provider.two:"+Integer.toString(kafkaPort));
		embeddedKafkaServers.add(embeddedKafkaServer);
	}
	

	
	private Producer<byte[], byte[]> buildProducer() {
		Properties props = new Properties();
		props.put(KafkaChannelConstants.METADATA_BROKER_LIST, seedBrokers.toString());
		props.put(KafkaChannelConstants.SERIALIZER_CLASS,serializer_class);
		props.put(KafkaChannelConstants.PARTITIONER_CLASS , partitioner_class);
		props.put(KafkaChannelConstants.REQUEST_REQUIRED_ACKS, request_required_acks);
		ProducerConfig config = new ProducerConfig(props);
		Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(config);
		
		return producer;
	}
	/**
	 * This test should perform e2e, the test will put the messages 
	 * to embedded kafka queue and will exepect same number of messages.
	 * @throws IOException
	 * @throws KafkaReaderException
	 * @throws InterruptedException
	 */
	@Test
	public void testReadDataFromKafka() throws IOException, KafkaReaderException, InterruptedException{
		String topic = "test-topic";
		List<String> testMessages = new ArrayList<String>();
		testMessages.add("Hello World 1");
		testMessages.add("Hello World 2");

		writeDataToKafkaQueue(testMessages,topic);
		String[] strings = seedBrokers.toString().split(",");
		KafkaSimpleConsumer simpleConsumer = KafkaSimpleConsumer.getInstance()
				.brokers(Arrays.asList(strings))
				.clientId("test-data-client-0")
				.topic(topic)
				.partitionId("0")
				.messageSize("100000")
				.build();
		simpleConsumer.start();
		writeDataToKafkaQueue(testMessages,topic);

		List<KafkaMessage> messages = simpleConsumer.pollData(simpleConsumer.getEarliestOffSet());
		Assert.assertEquals(testMessages.size(), messages.size());
		Thread.sleep(TimeUnit.MILLISECONDS.toMillis(1000));
	}
	
	private void writeDataToKafkaQueue(List<String> messages,String topic){
		// Build the producer that acts as the tracking API,
		// writing messages into the external Kafka cluster.
		//****************************************
		Producer<byte[], byte[]> producer = buildProducer();
		
		for(String message : messages){
			KeyedMessage<byte[], byte[]> keyedMessage = new KeyedMessage<byte[], byte[]>(
					topic, "key.0".getBytes(), message.getBytes());
			producer.send(keyedMessage);
		}
		producer.close();
	}
}