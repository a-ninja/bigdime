/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import java.util.Enumeration;
import java.util.List;

/**
 * Provides methods to get and update the alerts from a managed alert store.
 *
 * @author Neeraj Jain
 *
 */
public interface ManagedAlertService extends AlertService<ManagedAlert> {
	/**
	 * {@link Enumeration} of valid status values for an alert.
	 *
	 */
	public enum ALERT_STATUS {
		/**
		 * Alert was a false alarm.
		 */
		FALSE,

		/**
		 * Alert has been acknowledged.
		 */
		ACKNOWLEDGED,

		/**
		 * Alert has been resolved.
		 */
		RESOLVED
	}

	/**
	 * Update the status and comment for the given alert.
	 *
	 * @param alertMessage
	 *            alert for which the update operation is taking place
	 * @param alertStatus
	 *            new status of the alert
	 * @param comment
	 *            comment about the alert
	 * @return true if update was successful, false if the new and existng
	 *         statuses are same
	 */
	public boolean updateAlert(Alert alertMessage, ALERT_STATUS alertStatus, String comment) throws AlertException;
	
	/**
	 * @param alertServiceRequest
	 *        provides the request parameters based on which the alerts offset dates are fetched.
	 * @return list of dates as per the request criteria.       
	 */
	public List<Long> getDates(AlertServiceRequest alertServiceRequest) throws AlertException;

}
