/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.constants;

/**
 * @author jbrinnand
 */
public class KafkaChannelConstants {
	public static String TOPIC_ID = "topic.id";
	public static String PARTITION_ID = "partition.id";
	public static String BROKER_ID = "broker.id";
	public static String BROKERS = "brokers";
	public static String CLIENT_ID = "client.id";
	public static String  ZOOKEEPER_HOST = "zookeeper.host";
	public static String  ZOOKEEPER_PORT = "zookeeper.port";
	public static String  ZOOKEEPER_DEFAULT_ZNODE = "zookeeper.default.znode";
	public static String  LOG_DIRS = "log.dirs";
	public static String  LOG_RENTENTION = "log.retention";
	public static String  NUM_PARTITIONS = "num.partitions";
	public static String  TRANSACTION = "txn";
	public static String SERIALIZER_STRING_ENCODER = "kafka.serializer.StringEncoder";
	public static String TXN_TOPIC = "txn-topic";
	public static String NUM_REQUEST_REQUIRED_ACKS = "1";
	public static String MAX_TXN_CHANNELS = "5";
	public static String COLON_SEP = ":";
	public static String DASH = "-";
	public static String CHANNEL_TXN_CONSUMER = "channel-txn-consumer-";
	public static String PARTITION_ID_NUM = "0";
	public static String CHANNEL_PROVIDER_CONSUMER = "channel-provider-consumer-";
	public static String CONNECTOR_CHANNEL_CONSUMER = "connector-channel-consumer-";
	public static String OFFSET_DATA_DIR = "offsetDataDir";
	public static String NAME = "name";
	
	public static String METADATA_BROKER_LIST ="metadata.broker.list";  
	public static String SERIALIZER_CLASS = "serializer.class";
	public static String PARTITIONER_CLASS = "partitioner.class";
	public static String REQUEST_REQUIRED_ACKS = "request.required.acks";
	public static String REQUEST_TIMEOUT_MS = "request.timeout.ms";
	public static String DOT_SEP = ".";

	public static String TOPIC = "topic";
	public static String PARTITION = "partition";
	public static String KAFKA_HOST = "kafkaHost";
	public static String KAFKA_PORT = "kafkaPort";
	public static String SERIALIZER_CLAZZ = "serializerClass";
	public static String PARTITIONER_CLAZZ = "partitionerClass";
	public static String NUM_ACKS = "numAcks";
	public static String CHANNEL_ID = "name";
	public static String TYPE = "type";
	public static String REQUIRED_ACKS = "requiredAcks";
	public static String REQUEST_TIMEOUTMS = "requestTimeOutMs";
	public static String BROKER_LIST = "brokerList";
	public static String CHANNEL_EXTENSION = "-channel-txn.txt";
	public static String MESSAGE_SIZE = "messageSize";
	
	public static Integer TIMEOUT = 100000;
	public static Integer BUFFER_SIZE = 64 * 1024;
	public static Integer RETRIES = 3;
	public static String DEFAULT_PATH = "/tmp";
	public static String DEFAULT_MESSAGE_SIZE = "100000";
}
