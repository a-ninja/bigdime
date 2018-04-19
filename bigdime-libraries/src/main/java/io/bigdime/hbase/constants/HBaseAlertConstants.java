/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.constants;

import org.apache.hadoop.hbase.util.Bytes;
/**
 * 
 * @author mnamburi
 * 
 */

public final class HBaseAlertConstants {
	
	public static final byte[] ALERT_COLUMN_FAMILY_NAME = Bytes.toBytes("a");
	public static final byte[] ALERT_METADATA_COLUMN = Bytes.toBytes("m");
	public static final byte[] ALERT_COMMENT_COLUMN = Bytes.toBytes("c");
	//delete the string value for ALERT_HBASE_TABLE_NAME after testing
	public static final String ALERT_HBASE_TABLE_NAME = "bdp_splunk_meta";
	public static final String DAYS_PROPERTY_KEY = "days";
	public static final String FROM_DATE = "fromDate";
	public static final String TO_DATE = "toDate";
	public static final String ALERT_NAME_PROPERTY_KEY = "alertName";
	
	public static final String ALERT_MESSAGE="alertMessage";
	public static final String ALERT_STATUS="alertStatus";
	public static final String COMMENT="comment";
	
	
	public static final String INDEX_TIME_KEY = "_indextime";
	public static final String CD_KEY = "_cd";
	public static final String COMMENT_KEY = "comment";
	public static final String ROWKEY_KEY = "rowKey";
	public static final String RAW_KEY = "_raw";
	public static final String INTERPRETATION_KEY = "interpretation";
	
	public static final String RAISED_ALERTS_KEY = "raisedAlerts";
	
	public static final String PENDING = "pending";

}
