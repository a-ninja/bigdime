/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.constants;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * ApplicationsConstants encapsulates all the constants for bigdime-monitoring-service
 * @author Sandeep Reddy,Murthy
 *
 */

public class ApplicationConstants {
	
	public static final String ALERT_NAME="test_alert";
	public static final String SOURCE_TYPE="BIGDIME-MONITORING-SERVICE";
	public static final long LAST_N_DAYS=7*24*3600*1000l;
	public static final long WRONGDATEFORMAT=0l;
	public static final byte[] METADATA_COLUMN_FAMILY_NAME = Bytes.toBytes("cf");
	public static final byte[] METADATA_JSON_NAME = Bytes.toBytes("jn");
	public static final byte[] METADATA_NAME= Bytes.toBytes("nm");
	public static final byte[] METADATA_HANDLERNAME= Bytes.toBytes("hl");
	public static final byte[] METADATA_HANDLERCLASS= Bytes.toBytes("hc");
	public static final byte[] METADATA_SINKNAME= Bytes.toBytes("sl");
	public static final byte[] METADATA_SINKCLASS= Bytes.toBytes("sc");
	public static final byte[] METADATA_CHANNEL= Bytes.toBytes("cl");
	public static final byte[] METADATA_NONDEFAULTS= Bytes.toBytes("nd");
	public static final byte[] METADATA_MANDATORYFIELDS= Bytes.toBytes("mf");
	public static final byte[] METADATA_ENVIRONMENT= Bytes.toBytes("en");
	public static final byte[] METADATA_NONESSENTIALFIELDS= Bytes.toBytes("nf");


}
