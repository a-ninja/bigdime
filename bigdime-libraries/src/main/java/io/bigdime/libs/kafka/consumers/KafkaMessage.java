/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.consumers;

/**
 * 
 * @author mnamburi
 *
 */
public class KafkaMessage {
	private String topicName;
	private int partition;
	private long offset;
	private byte[] message;
	//TODO : may use byte buffer?
	public static KafkaMessage getInstance(String topicName,int partition,long offset,byte[] message){
		return new KafkaMessage(topicName,partition,offset,message);
	}
	
	public KafkaMessage(){
	}

	public KafkaMessage(String topicName,int partition,long offset,byte[] message){
		this.topicName = topicName;
		this.topicName = topicName;
		this.offset = offset;
		this.message = message;
	}
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	public int getPartition() {
		return partition;
	}
	public void setPartition(int partition) {
		this.partition = partition;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public byte[] getMessage() {
		return message;
	}
	public void setMessage(byte[] message) {
		this.message = message;
	}
}
