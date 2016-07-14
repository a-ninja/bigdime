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
	public static final String KERBEROS_USER_NAME = "kerberos-user-name";
	public static final String KERBEROS_KEYTAB_PATH = "kerberos-keytab-path";

}
