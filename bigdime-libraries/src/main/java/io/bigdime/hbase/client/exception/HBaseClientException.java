/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client.exception;
/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
public class HBaseClientException extends Exception {
	private static final long serialVersionUID = 1L;
	public HBaseClientException(String message) {
		super(message);
	}
	public HBaseClientException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
