/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

/**
 * This exception is thrown if the validator encounters any problems in
 * performing validation.
 * 
 * @author Neeraj Jain
 *
 */
public class DataValidationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataValidationException(String message) {
		super(message);
	}

	public DataValidationException(String message, Throwable t) {
		super(message, t);
	}

}
