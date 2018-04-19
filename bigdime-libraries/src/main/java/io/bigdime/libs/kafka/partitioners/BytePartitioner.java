/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.partitioners;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 
 * @author anaithani/mnamburi
 *  When the message.key has posted to kafka,
 *   this partitioner code base to support parsing the message and generate the partiton number.
 *   
 *   example : messageKey : "key.0"
 *   		   message : "Hello World"
 *   
 *   The BytePartitioner class will parse the key and retrive the number after ".", return to the caller.
 */
public class BytePartitioner implements Partitioner {

	public BytePartitioner(VerifiableProperties props) {
	}

	public int partition(Object key, int numPartitions) {
		int partition = 0;
		if (!(key instanceof byte[])) {
			throw new IllegalArgumentException("Key should be type of byte[]");
		}
		byte[] byteKey = (byte[]) key;
		String stringKey = new String(byteKey);
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(stringKey.substring(offset + 1))
					% numPartitions;
		}
		return partition;
	}
}
