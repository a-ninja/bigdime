/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

/**
 * ActionEvent extends {@link SimpleEvent} and used in several method
 * signatures.
 *
 * @author Neeraj Jain
 */
public class ActionEvent extends SimpleEvent {
	private Status status = Status.READY;

	public ActionEvent() {

	}

	public ActionEvent(Status status) {
		this.status = status;
	}

	public ActionEvent(Event event) {
		setBody(event.getBody());
		setHeaders(event.getHeaders());
	}

	@Override
	public String toString() {
		return "[Event headers = " + getHeaders() + ", body = " + new String(getBody(), Charset.defaultCharset())
				+ "status=" + status + " ]";
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public static enum Status {
		/**
		 * Done processing the current data set(file, or message etc).
		 */
		READY,

		/**
		 * Partially done processing the current data set, callback before
		 * sending new data.
		 */
		CALLBACK,

		/**
		 * No data available, cool off. Wait until more data is available from
		 * data source; be it a channel or database or kafka etc. This should be
		 * renamed to BACKOFF_WAIT.
		 */
		BACKOFF,

		/**
		 * No data available to process with the current handler, but may be
		 * available in other handlers prior to this. So, go back, immediately,
		 * without having to wait for the data to be available. Difference
		 * between BACKOFF and BACKOFF_NOW is that BACKOFF will cause handler
		 * manager to wait for sometime before continuing while BACKOFF_NOW will
		 * continue immediately.
		 */
		BACKOFF_NOW

	}

	public Status getStatus() {
		return status;
	}

	public static ActionEvent newBackoffEvent() {
		return new ActionEvent(Status.BACKOFF);
	}

}
