package io.bigdime.handler.hive;

import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;

public class HiveJdbcReaderHandlerConfig {

	private String jdbcUrl = null;// e.g.
									// jdbc:hive2://host:10000/default;principal=hadoop/host@DOMAIN.COM
	private String driverClassName = null;
	private HDFS_AUTH_OPTION authOption;

	private String userName = null;// e.g. username
	private String password = null; // e.g. password

	private String baseOutputDirectory = null;

	private String entityName = null;
	private String hiveQuery = null;

	/**
	 * How many days should we go back to process the records. 0 means process
	 * todays records, 1 means process yesterdays records
	 */
	private int goBackDays;

	/**
	 * Go back atleast these many millis.
	 * 
	 * <br>
	 * 1 day means, process records from yesterday and before.
	 * 
	 */
	private long minGoBack;

	/**
	 * Latency between source data and data available in hadoop.
	 * 
	 * <br>
	 * 12 hours means, it takes 12 hours to get the data in hdfs from the live
	 * data source. Default value is 0.
	 */
	private long latency;
	/**
	 * intervalInMins, cron expression and goBackDays properties are highly
	 * dependent on each other. As a thumb rule, intervalInMins must be same as
	 * frequency set in cron expression.
	 * 
	 * If the cron expression is to run every minute and the intervalInMins is
	 * set to, say, 1 day, the reader will wait for 1 day to proceed.
	 * 
	 * intervalInMins is needed because of the goBackDays property. If
	 * goBackDays is set to 10 days, then the reader reads the 10 days old data
	 * during the first run. After the first run, it adds intervalInMins to the
	 * get to read the 9 days old data and so on.
	 */
	// private long intervalInMins = 24 * 60;// default to a day
	// private long intervalInMillis = intervalInMins * 60 * 1000;

	private String frequency;

	private String outputDirectoryPattern;

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public HDFS_AUTH_OPTION getAuthOption() {
		return authOption;
	}

	public void setAuthOption(HDFS_AUTH_OPTION authOption) {
		this.authOption = authOption;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getBaseOutputDirectory() {
		return baseOutputDirectory;
	}

	public void setBaseOutputDirectory(String baseOutputDirectory) {
		this.baseOutputDirectory = baseOutputDirectory;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getHiveQuery() {
		return hiveQuery;
	}

	public void setHiveQuery(String hiveQuery) {
		this.hiveQuery = hiveQuery;
	}

	public int getGoBackDays() {
		return goBackDays;
	}

	public void setGoBackDays(int goBackDays) {
		this.goBackDays = goBackDays;
	}

	public String getOutputDirectoryPattern() {
		return outputDirectoryPattern;
	}

	public void setOutputDirectoryPattern(String outputDirectoryPattern) {
		this.outputDirectoryPattern = outputDirectoryPattern;
	}

	public String getFrequency() {
		return frequency;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	public long getMinGoBack() {
		return minGoBack;
	}

	public void setMinGoBack(long minGoBack) {
		this.minGoBack = minGoBack;
	}

	public long getLatency() {
		return latency;
	}

	public void setLatency(long latency) {
		this.latency = latency;
	}

	@Override
	public String toString() {
		return "HiveJdbcReaderHandlerConfig [jdbcUrl=" + jdbcUrl + ", driverClassName=" + driverClassName
				+ ", authOption=" + authOption + ", userName=" + userName + ", password=" + password
				+ ", baseOutputDirectory=" + baseOutputDirectory + ", entityName=" + entityName + ", hiveQuery="
				+ hiveQuery + ", goBackDays=" + goBackDays + ", minGoBack=" + minGoBack + ", latency=" + latency
				+ ", frequency=" + frequency + ", outputDirectoryPattern=" + outputDirectoryPattern + "]";
	}
}
