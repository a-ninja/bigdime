/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import java.util.Date;

/**
 * Defines an object to provide fields needed to query alerts.
 *
 * @author Neeraj Jain,Sandeep reddy Murthy
 *
 */
public class AlertServiceRequest {

	/**
	 * Unique identifier for the alert.
	 */
	private String alertId;
	/**
	 * If the request is to fetch the alerts within a date range, this is the
	 * start of the range.
	 */
	private Date fromDate;
	/**
	 * If the request is to fetch the alerts within a date range, this is the
	 * end of the range.
	 */
	private Date toDate;
	/**
	 * Offset, needed for pagination.
	 */
	private int offset;
	/**
	 * number of messages to fetch, max is 50.
	 */
	private int limit;
	/**
	 * search criteria for the alerts to be fetched with.
	 */
	private String search;
	
	/**
	 * Get the alert id, can be used to get the alert conditions.
	 *
	 * @return
	 */
	public String getAlertId() {
		return alertId;
	}

	/**
	 * Set the alert id for this request.
	 *
	 * @param alertId
	 */
	public void setAlertId(String alertId) {
		this.alertId = alertId;
	}

	/**
	 * Get the fromDate, represents the start of the date range. Default value
	 * is now-24hours.
	 *
	 * @return fromDate
	 */
	public Date getFromDate() {
		return new Date(fromDate.getTime());
	}

	/**
	 * Get the fromDate, represents the start of the date range.
	 *
	 * @param fromDate
	 */
	public void setFromDate(Date fromDate) {
		this.fromDate = new Date(fromDate.getTime());
	}

	public Date getToDate() {
		return new Date(toDate.getTime());
	}

	public void setToDate(Date toDate) {
		this.toDate = new Date(toDate.getTime());
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public String getSearch() {
		return search;
	}

	public void setSearch(String search) {
		this.search = search;
	}

}
