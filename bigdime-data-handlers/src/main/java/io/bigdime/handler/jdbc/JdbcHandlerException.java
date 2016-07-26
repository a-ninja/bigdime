/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import io.bigdime.core.HandlerException;

/**
 * Jdbc Handler Exception is thrown when the application is not able to
 * process the rdbms data.
 * 
 * @author Pavan Sabinikari
 * 
 */
public class JdbcHandlerException extends HandlerException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public JdbcHandlerException(Throwable sourceException) {
		super(sourceException);
	}

	public JdbcHandlerException(String errorMessage) {
		super(errorMessage);

	}

}

