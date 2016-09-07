package io.bigdime.handler;

public class JobStatusException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public JobStatusException(final String message) {
		super(message);
	}

	public JobStatusException(final String message, final Exception e) {
		super(message, e);
	}

}
