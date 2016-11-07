package io.bigdime.handler.hive;

public enum HiveJdbcReaderHandlerConstants {

	INSTANCE;

	public static HiveJdbcReaderHandlerConstants getInstance() {
		return HiveJdbcReaderHandlerConstants.INSTANCE;
	}

	public static final String ENTITY_NAME = "entity-name";
	public static final String BASE_OUTPUT_DIRECTORY = "base-output-directory";
	public static final String HIVE_CONF = "hive-conf";
	public static final String HIVE_QUERY = "hive-query";

	public static final String JDBC_URL = "jdbc-connection-url";
	public static final String DRIVER_CLASS_NAME = "driver-class-name";

	public static final String AUTH_CHOICE = "auth-choice";

	public static final String GO_BACK_DAYS = "go-back-days";
	public static final String MIN_GO_BACK = "min-go-back";

	public static final String HIVE_JDBC_USER_NAME = "hive-jdbc-user-name";
	public static final String HIVE_JDBC_SECRET = "hive-jdbc-secret";

	public static final String OUTPUT_DIRECTORY_DATE_FORMAT = "output-directory-date-format";
	public static final String HIVE_QUERY_DATE_FORMAT = "hive-query-date-format";
	public static final String FREQUENCY = "frequency";
	public static final String YARN_CONF = "yarn-conf";
	public static final String SLEEP_BETWEEN_RETRY_SECONDS = "sleep-between-retry-seconds";
	public static final String MAX_RETRIES = "max-retries";
	public static final String LATENCY = "latency";
	public static final String YARN_SITE_XML_PATH = "yarn-site-xml-path";
	public static final String TOUCH_FILE = "touch-file";
	
}
