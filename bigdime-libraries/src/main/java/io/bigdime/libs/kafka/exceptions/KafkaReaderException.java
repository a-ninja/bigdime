/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.exceptions;

/**
 * 
 * @author mnamburi
 *
 */
public class KafkaReaderException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public KafkaReaderException(String message) {
		super(message);
	}
	public KafkaReaderException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
