/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.consumers;

import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.BUFFER_SIZE;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.CHANNEL_EXTENSION;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.CLIENT_ID;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.DEFAULT_MESSAGE_SIZE;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.DEFAULT_PATH;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.MESSAGE_SIZE;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.OFFSET_DATA_DIR;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.PARTITION_ID;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.TIMEOUT;
import static io.bigdime.libs.kafka.constants.KafkaChannelConstants.TOPIC_ID;
import io.bigdime.libs.kafka.exceptions.KafkaReaderException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This producer is used by the channel to send events to a Kafka broker.
 * The Kafka Simple Consumber input is list of the brokers or at least one broker.
 * a Topic information from the message to be read?
 * list of the Kafka propeties (message Size ..etc)
 * @author jbrinnand/mnamburi
 *
 */
public class KafkaSimpleConsumer {
	private static Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumer.class);
	private Properties properties;
	private SimpleConsumer consumer;
	private List<String> m_replicaBrokers = new ArrayList<String>();
	private List<String> brokers;
	private String leadBroker;

	private Path file;
	private String basePath;
	private String pathExtension;
	private boolean isStarted;
	private int messageSize;

	protected KafkaSimpleConsumer() {
		properties = new Properties();
		basePath = DEFAULT_PATH; 
		pathExtension = CHANNEL_EXTENSION; 
		isStarted = false;
		messageSize = Integer.valueOf(DEFAULT_MESSAGE_SIZE);
	}

	public static KafkaSimpleConsumer getInstance() {
		return new KafkaSimpleConsumer();
	}

	public KafkaSimpleConsumer brokers(List<String> brokers) {
		this.brokers = brokers;
		return this;
	}
	public KafkaSimpleConsumer topic(String topic) {
		properties.put(TOPIC_ID, topic);
		return this;
	}

	public KafkaSimpleConsumer partitionId(String partitionId) {
		properties.put(PARTITION_ID, partitionId);
		return this;
	}

	public KafkaSimpleConsumer clientId(String clientId) {
		properties.put(CLIENT_ID, clientId);
		return this;
	}
	public KafkaSimpleConsumer offSetDataDir(String offsetDataDir) {
		properties.put(OFFSET_DATA_DIR, offsetDataDir);
		return this;
	}
	public KafkaSimpleConsumer messageSize(String messageSize) {
		properties.put(MESSAGE_SIZE, messageSize);
		return this;
	}

	public KafkaSimpleConsumer properties(Properties props){
		properties.putAll(props);
		return this;
	}
	/**
	 * TODO : Do we need to define Partition ID ?
	 * @return
	 * @throws IOException
	 */
	public KafkaSimpleConsumer build() {
		Preconditions.checkNotNull(brokers);
		Preconditions.checkNotNull(properties.getProperty(TOPIC_ID));
		Preconditions.checkNotNull(properties.getProperty(PARTITION_ID));

		if(properties.getProperty(OFFSET_DATA_DIR) != null){
			basePath = properties.getProperty(OFFSET_DATA_DIR);
		}

		if(properties.getProperty(MESSAGE_SIZE) != null){
			messageSize =Integer.valueOf(properties.getProperty(MESSAGE_SIZE));
		}

		electLeader();
		return this;
	}
	/**
	 * this method supports to find latest leader for corresponding partition and reset the consumer
	 */
	public void electLeader(){
		PartitionMetadata metadata = null; 
		metadata = findPartitionReplicaLeader();
		if (metadata == null || metadata.leader() == null) {
			leadBroker = null;
		} else {
			leadBroker = metadata.leader().host();
			consumer = new SimpleConsumer(metadata.leader().host(), 
					metadata.leader().port(), TIMEOUT, BUFFER_SIZE,
					properties.getProperty(CLIENT_ID));
			logger.info("KDA Provider cluster - topic:partion [{},{}] "+ "lead broker: {}, replicas: {}",
					properties.get(TOPIC_ID), metadata.partitionId(),
					metadata.leader(), metadata.isr().toString());
		}
	}

	/**
	 * Need to store these informations in metastore or another Kafka Queue.
	 * @throws IOException
	 * @throws KafkaReaderException 
	 */
	@Deprecated
	public void start() throws IOException, KafkaReaderException {
		init();
		isStarted = true;
		logger.info("Client id: {} initialization complete - start state is: {} currentOffset is: {} ",
				properties.getProperty("client.id"), isStarted);
	}

	public void stop() {
		logger.info("Consumer : {} shutting down.", properties.get(CLIENT_ID));
		consumer.close();
	}
	/**
	 * polls data from Kafka Queue and build a kafka message with topic/offset/partition/message bytes.
	 * 	
	 *  Possible error code:
	 *
	 *  OFFSET_OUT_OF_RANGE (1)
	 *  UNKNOWN_TOPIC_OR_PARTITION (3)
	 *  NOT_LEADER_FOR_PARTITION (6)
	 *  REPLICA_NOT_AVAILABLE (9)
	 *  UNKNOWN (-1)
	 *
	 * @return
	 * @throws KafkaReaderException 
	 * @throws InterruptedException
	 */
	public List<KafkaMessage> pollData(long readOffset) throws KafkaReaderException {
		String clientId = properties.getProperty("client.id");
		String topicId = properties.getProperty("topic.id");
		int partitionId = Integer.valueOf((String) properties.getProperty("partition.id"));
		byte[] bytes = null;
		KafkaMessage kafkaMessage = null;
		List<KafkaMessage> messages = new ArrayList<KafkaMessage>();
		FetchRequest req = new FetchRequestBuilder().clientId(clientId)
				.addFetch(topicId, partitionId, readOffset, messageSize).build();
		FetchResponse fetchResponse = null;
		fetchResponse = consumer.fetch(req);

		if (fetchResponse.hasError()) {
			short code = fetchResponse.errorCode(topicId, partitionId);
			if(code == Errors.LEADER_NOT_AVAILABLE.code() || code == Errors.NOT_LEADER_FOR_PARTITION.code() ){
				electLeader();
			}
			logger.error(
					"Error fetching data from the Broker :\" clientId={} topicId={} partitionId={} "
							+ "currentOffset={} leadBroker={} fetchResponseCode={}", clientId, topicId ,partitionId,readOffset,leadBroker,Errors.forCode(code).name());

			throw new KafkaReaderException("Error fetching data from the Broker : leadBroker "+leadBroker+ "error = "+Errors.forCode(code).name());
		}

		for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
				topicId, partitionId)) {
			readOffset = messageAndOffset.offset();
			logger.debug(clientId + "." + topicId + ":" + String.valueOf(partitionId) + "."
					+ "Read offset {} ", readOffset);

			ByteBuffer payload = messageAndOffset.message().payload();
			bytes = new byte[payload.limit()];
			payload.get(bytes);
			kafkaMessage = KafkaMessage.getInstance(topicId, partitionId, readOffset, bytes);
			messages.add(kafkaMessage);
		}
		logger.debug(clientId + "." + topicId + ":" + String.valueOf(partitionId) + "," + "Read offset {} ", readOffset);
		return messages;
	}
	/**
	 * this method supports to read an offset from Kafka Queue.
	 * @return
	 * @throws KafkaReaderException
	 */
	public long getEarliestOffSet() throws  KafkaReaderException {
		String clientId = properties.getProperty(CLIENT_ID);
		String topicId = properties.getProperty(TOPIC_ID);
		int partitionId = Integer.valueOf((String) properties.getProperty(PARTITION_ID));
		TopicAndPartition topicAndPartition = new TopicAndPartition(topicId,
				partitionId);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = 
				new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.EarliestTime(), 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientId);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			short code = response.errorCode(topicId, partitionId);

			logger.error(
					"Error reading Offset from the Broker :\" clientId={} topicId={} partitionId={} "
							+ "leadBroker={} fetchResponseCode={}", clientId, topicId ,partitionId,leadBroker,Errors.forCode(code).name());
			throw new KafkaReaderException("Error reading Offset from the Broker : leadBroker "+leadBroker+ "error = "+Errors.forCode(code).name());
		}
		long[] offsets = response.offsets(topicId, partitionId);
		return offsets[0];
	}
	/**
	 * 
	 * @return
	 * @throws KafkaReaderException 
	 */
	public long getLastOffset() throws KafkaReaderException {
		String clientName = properties.getProperty(CLIENT_ID);
		String topicName = properties.getProperty(TOPIC_ID);
		int partitionId = Integer.valueOf((String) properties.getProperty(PARTITION_ID));

		logger.info("Consumer {} getting offset for :  {}:{}", 
				clientName, consumer.host(), consumer.port());
		TopicAndPartition topicAndPartition = new TopicAndPartition(topicName,
				partitionId);

		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = 
				new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.LatestTime(), 1));

		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);

		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			short code = response.errorCode(topicName, partitionId);
			logger.error(
					"Error reading Offset from the Broker :\" clientId={} topicId={} partitionId={} "
							+ "currentOffset={} leadBroker={} fetchResponseCode={}", clientName, topicName ,partitionId,leadBroker,Errors.forCode(code).name());
			throw new KafkaReaderException("Error reading Offset from the Broker : leadBroker "+leadBroker+ "error = "+Errors.forCode(code).name());
		}
		long[] offsets = response.offsets(topicName, partitionId);
		return offsets[0];
	}

	@Deprecated
	private long init() throws IOException, KafkaReaderException {
		// Get the path from the configuration.
		// Check the currentOffset that is saved in a durable
		// store. If it does not exist (it means that this is a new instance),
		// so get the earliest offset from Kafka. Otherwise, use the
		// offset from the durable store.
		// **************************************************
		long offset;
		String clientId = properties.getProperty(CLIENT_ID);

		String absolutePath = basePath + "/" + clientId + pathExtension;
		Path path = Paths.get(absolutePath);
		Path basePathAbsPath = Paths.get(basePath);

		if(!Files.exists(basePathAbsPath)){
			FileUtils.forceMkdir(basePathAbsPath.toFile());
		}
		if (!Files.exists(path)) {
			file = Files.createFile(path);
		} else {
			file = FileSystems.getDefault().getPath(absolutePath);
		}
		byte[] fileStoredOffset = Files.readAllBytes(file);
		String storedOffset = new String(fileStoredOffset);
		if (storedOffset != null && storedOffset.contains("currentOffset")) {
			offset = Long.valueOf(storedOffset.split(":")[1].trim()).longValue();
		} else {
			offset = getLastOffset();	
		}
		return offset;
	}
	// TODO : thrown an error if host is not reachable.
	public PartitionMetadata findPartitionReplicaLeader () {
		String topic = String.valueOf(properties.get(TOPIC_ID));
		int partition = Integer.valueOf((String)properties.get(PARTITION_ID)).intValue();

		PartitionMetadata metadata = null; 
		for (String broker : brokers) {
			String[] brokerPorts = broker.split(":");
			List<String> seedBrokers = new ArrayList<String>();
			seedBrokers.add(brokerPorts[0]);
			int port = Integer.valueOf(brokerPorts[1]);
			metadata = findLeader(seedBrokers, port, topic, partition);
			if (metadata != null) {
				// Note: this assertion requires a replication factor of two.
				//*********************************************
				logger.info("KDA Provider cluster - topic:partion [ {}, {}]" +  "lead broker: {}",
						topic, metadata.partitionId(), metadata.leader()); 
				logger.info("KDA Provider cluster - topic:partion [ {}, {}]" +  "replicas: {}",
						topic, metadata.partitionId(), metadata.isr().toString());
				break;
			}
		}
		return metadata;
	}
	private PartitionMetadata findLeader(List<String> a_seedBrokers,
			int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		for (String seed : a_seedBrokers) {
			try {
				String clientId = (String) properties.get(CLIENT_ID);
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
						clientId);
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break;
						}
					}
				}
			} catch (Exception e) {
				logger.error("Error communicating with Broker [" + seed + "] "
						+ "to find Leader for [" + a_topic + ", " + a_partition
						+ "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}


	public boolean isStarted() {
		return isStarted;
	}
}
