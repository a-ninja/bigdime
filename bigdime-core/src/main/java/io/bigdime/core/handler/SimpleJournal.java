/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.List;

import io.bigdime.core.ActionEvent;

public class SimpleJournal implements HandlerJournal {

	private long totalRead;
	private long totalSize;
	private long readCount;
	private List<ActionEvent> eventList;

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.handler.HandlerJournal#getTotalRead()
	 */
	@Override
	public long getTotalRead() {
		return totalRead;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.handler.HandlerJournal#setTotalRead(long)
	 */
	@Override
	public void setTotalRead(long totalRead) {
		this.totalRead = totalRead;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.handler.HandlerJournal#getTotalSize()
	 */
	@Override
	public long getTotalSize() {
		return totalSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.handler.HandlerJournal#setTotalSize(long)
	 */
	@Override
	public void setTotalSize(long totalSize) {
		this.totalSize = totalSize;
	}

	@Override
	public String toString() {
		return "SimpleJournal [totalRead=" + totalRead + ", totalSize=" + totalSize + "]";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.handler.HandlerJournal#getEventList()
	 */
	@Override
	public List<ActionEvent> getEventList() {
		return eventList;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.bigdime.core.handler.HandlerJournal#setEventList(java.util.List)
	 */
	@Override
	public void setEventList(List<ActionEvent> eventList) {
		this.eventList = eventList;
	}

	public void reset() {
		setTotalRead(0);
		setEventList(null);
		setTotalSize(0);
		setReadCount(0);
	}

	public long getReadCount() {
		return readCount;
	}

	public void setReadCount(long readCount) {
		this.readCount = readCount;
	}
}
