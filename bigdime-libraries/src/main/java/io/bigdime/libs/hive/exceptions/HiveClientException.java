/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.exceptions;

public class HiveClientException extends Exception {
	private static final long serialVersionUID = 1L;
	public HiveClientException(String message) {
		super(message);
	}
	public HiveClientException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
