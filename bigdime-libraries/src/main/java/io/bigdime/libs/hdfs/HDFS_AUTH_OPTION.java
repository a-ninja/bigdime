package io.bigdime.libs.hdfs;

public enum HDFS_AUTH_OPTION {
	KERBEROS, PASSWORD;

	public static HDFS_AUTH_OPTION getByName(final String arg) {
		if (arg == null)
			return null;
		if (arg.equalsIgnoreCase(KERBEROS.toString()))
			return KERBEROS;
		if (arg.equalsIgnoreCase(PASSWORD.toString()))
			return PASSWORD;
		return null;
	}

}
