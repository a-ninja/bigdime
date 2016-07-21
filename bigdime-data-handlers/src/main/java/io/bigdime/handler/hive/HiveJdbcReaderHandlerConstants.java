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

	public enum AUTH_OPTION {
		KERBEROS, PASSWORD;

		public static AUTH_OPTION getByName(final String arg) {
			if (arg == null)
				return null;
			if (arg.equalsIgnoreCase(KERBEROS.toString()))
				return KERBEROS;
			if (arg.equalsIgnoreCase(PASSWORD.toString()))
				return PASSWORD;
			return null;
		}
	}

	public static final String HIVE_JDBC_USER_NAME = "hive-jdbc-user-name";
	public static final String HIVE_JDBC_SECRET = "hive-jdbc-secret";

}
