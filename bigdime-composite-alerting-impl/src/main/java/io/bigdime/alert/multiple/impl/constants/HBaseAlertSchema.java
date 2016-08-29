/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.multiple.impl.constants;

public class HBaseAlertSchema {

	public static class ColumnFamily {
		public static final String ALERT_COLUMN_FAMILY = "cf";
	}

	public static class ColumnQualifier {
		public static final String ALERT_ADAPTOR_NAME = "an";
		public static final String ALERT_MESSAGE_CONTEXT = "cx";
		public static final String ALERT_ALERT_CODE = "cd";
		public static final String ALERT_ALERT_NAME = "al";
		public static final String ALERT_ALERT_CAUSE = "er";
		public static final String ALERT_ALERT_SEVERITY = "sv";
		public static final String ALERT_ALERT_MESSAGE = "ms";
		public static final String ALERT_ALERT_DATE = "dt";
		public static final String ALERT_ALERT_EXCEPTION = "ex";
		public static final String ALERT_HOST_NAME = "hn";
		public static final String ALERT_HOST_IP = "ip";
		public static final String LOG_LEVEL = "lv";
	}
}
