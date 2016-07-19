/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.swift;

import java.util.List;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.handler.SimpleJournal;

public class SwiftWriterHandlerJournal extends SimpleJournal {
	private String currentSwiftObjectName = "";
	private int recordCount = -1;
	private List<ActionEvent> eventList;

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public void incrementRecordCount() {
		this.recordCount++;
	}

	public List<ActionEvent> getEventList() {
		return eventList;
	}

	public void setEventList(List<ActionEvent> eventList) {
		this.eventList = eventList;
	}

	public void reset() {
		setRecordCount(0);
		setEventList(null);
	}

	public String getCurrentSwiftObjectName() {
		return currentSwiftObjectName;
	}

	public void setCurrentSwiftObjectName(String currentSwiftObjectName) {
		this.currentSwiftObjectName = currentSwiftObjectName;
	}

	@Override
	public String toString() {
		return "SwiftWriterHandlerJournal [currentSwiftObjectName=" + currentSwiftObjectName + ", recordCount="
				+ recordCount + ", eventList=" + eventList + "]";
	}
}