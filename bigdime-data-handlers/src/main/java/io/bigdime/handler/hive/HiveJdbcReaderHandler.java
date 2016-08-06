package io.bigdime.handler.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.ProcessHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;
import io.bigdime.libs.hdfs.jdbc.HiveJdbcConnectionFactory;

/**
 * 
 * HiveReaderHandler reads data from a hive table/query and outputs each row as
 * one event.
 * 
 * HiveReaderHandler: Configure one source for each recurring file. Store data
 * in file. Send the output filename in the header. File reader handler to get
 * the filename from the header. SwiftWriter
 * 
 * src-desc : { entity-name query
 * 
 * "input1": { "entity-name": "tracking_events", "hive-query": "query"
 * "hive-conf": { "mapred.job.queue.name" : "queuename",
 * "mapred.output.compress" : "true", "hive.exec.compress.output" : "true",
 * "mapred.output.compression.codec" :
 * "org.apache.hadoop.io.compress.GzipCodec", "io.compression.codecs" :
 * "org.apache.hadoop.io.compress.GzipCodec", "mapred.reduce.tasks" : "500"
 * 
 * "schemaFileName" :"${hive_schema_file_name}" },
 * 
 * "input1" : "table1: table1 ", "input2" : "table2: table2"
 * 
 * 
 * }
 * 
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public final class HiveJdbcReaderHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HiveJdbcReaderHandler.class));
	private String outputDirectory = "";
	final Map<String, String> hiveConfigurations = new HashMap<>();
	@Autowired
	HiveJdbcConnectionFactory hiveJdbcConnectionFactory;
	private Connection connection = null;
	private HiveReaderDescriptor inputDescriptor;
	private long hiveConfDateTime;

	private static final int MILLS_IN_A_DAY = 24 * 60 * 60 * 1000;
	private long intervalInMins = 24 * 60;// default to a day
	private long intervalInMillis = intervalInMins * 60 * 1000;

	private static final String OUTPUT_DIRECTORY_DATE_FORMAT = "yyyy-MM-dd";

	private static final String INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.hive.HiveJdbcReaderHandler";
	final private int DEFAULT_GO_BACK_DAYS = 1;
	private HiveJdbcReaderHandlerConfig handlerConfig = new HiveJdbcReaderHandlerConfig();

	final DateTimeFormatter jobDtf = DateTimeFormat.forPattern("yyyyMMdd-HHmmss.SSS");
	final DateTimeFormatter hiveQueryDtf = DateTimeFormat.forPattern("yyyy-MM-dd");
	private DateTimeFormatter hdfsOutputPathDtf;
	public static final String FORWARD_SLASH = "/";

	@Override
	public void build() throws AdaptorConfigurationException {
		setHandlerPhase("building HiveJdbcReaderHandler");
		super.build();
		logger.info(getHandlerPhase(), "properties={}", getPropertyMap());

		String jdbcUrl = null;
		String driverClassName = null;
		HDFS_AUTH_OPTION authOption;

		String userName = null;
		String password = null;

		String baseOutputDirectory = null;

		String entityName = null;
		String hiveQuery = null;

		/**
		 * How many days should we go back to process the records. 0 means
		 * process todays records, 1 means process yesterdays records
		 */
		int goBackDays = DEFAULT_GO_BACK_DAYS;

		Map<String, Object> properties = getPropertyMap();
		for (String key : properties.keySet()) {
			logger.info(getHandlerPhase(), "key=\"{}\" value=\"{}\"", key, getPropertyMap().get(key));
		}

		// sanity check for src-desc
		@SuppressWarnings("unchecked")
		Entry<Object, String> srcDescEntry = (Entry<Object, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescEntry == null) {
			throw new InvalidValueConfigurationException("src-desc can't be null");
		}

		logger.info(getHandlerPhase(), "src-desc-node-key=\"{}\" src-desc-node-value=\"{}\"", srcDescEntry.getKey(),
				srcDescEntry.getValue());

		@SuppressWarnings("unchecked")
		Map<String, Object> srcDescValueMap = (Map<String, Object>) srcDescEntry.getKey();

		entityName = PropertyHelper.getStringProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.ENTITY_NAME);
		hiveQuery = PropertyHelper.getStringProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_QUERY);

		goBackDays = PropertyHelper.getIntProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.GO_BACK_DAYS,
				DEFAULT_GO_BACK_DAYS);

		logger.debug(getHandlerPhase(), "entityName=\"{}\" hiveQuery=\"{}\" goBackDays={}", entityName, hiveQuery,
				goBackDays);

		Preconditions.checkArgument(goBackDays >= 0,
				HiveJdbcReaderHandlerConstants.GO_BACK_DAYS + " has to be a non-negative value.");

		String frequencyExpression = PropertyHelper.getStringProperty(srcDescValueMap,
				HiveJdbcReaderHandlerConstants.FREQUENCY);
		for (String key : srcDescValueMap.keySet()) {
			logger.debug(getHandlerPhase(), "srcDesc-key=\"{}\" srcDesc-value=\"{}\"", key, srcDescValueMap.get(key));
		}

		setHiveConfigurations(srcDescValueMap);

		// Set JDBC params
		jdbcUrl = PropertyHelper.getStringProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.JDBC_URL);
		driverClassName = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.DRIVER_CLASS_NAME);
		final String authChoice = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString());

		authOption = HDFS_AUTH_OPTION.getByName(authChoice);

		userName = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.HIVE_JDBC_USER_NAME);
		password = PropertyHelper.getStringProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.HIVE_JDBC_SECRET);

		baseOutputDirectory = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.BASE_OUTPUT_DIRECTORY, "/");
		String outputDirectoryPattern = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.OUTPUT_DIRECTORY_DATE_FORMAT, OUTPUT_DIRECTORY_DATE_FORMAT);
		hdfsOutputPathDtf = DateTimeFormat.forPattern(outputDirectoryPattern);

		logger.info(getHandlerPhase(),
				"jdbcUrl=\"{}\" driverClassName=\"{}\" authChoice={} authOption={} userName=\"{}\" password=\"****\" baseOutputDirectory={} frequencyExpression={}",
				jdbcUrl, driverClassName, authChoice, authOption, userName, baseOutputDirectory, outputDirectoryPattern,
				frequencyExpression);
		handlerConfig.setAuthOption(authOption);
		handlerConfig.setBaseOutputDirectory(baseOutputDirectory);
		handlerConfig.setDriverClassName(driverClassName);
		handlerConfig.setEntityName(entityName);
		handlerConfig.setGoBackDays(goBackDays);
		handlerConfig.setHiveQuery(hiveQuery);
		handlerConfig.setJdbcUrl(jdbcUrl);
		handlerConfig.setPassword(password);
		handlerConfig.setUserName(userName);
		logger.info(getHandlerPhase(),
				"jdbcUrl=\"{}\" driverClassName=\"{}\" authChoice={} authOption={} userName=\"{}\" password=\"****\" baseOutputDirectory={}",
				getJdbcUrl(), getDriverClassName(), authChoice, getAuthOption(), getUserName(),
				getBaseOutputDirectory(), getOutputDirectoryPattern());
	}

	@SuppressWarnings("unchecked")
	private void setHiveConfigurations(Map<String, Object> srcDescValueMap) {
		if (getPropertyMap().get(HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
			logger.debug(getHandlerPhase(), "found hive-conf in handler properties");
			hiveConfigurations
					.putAll(PropertyHelper.getMapProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.HIVE_CONF));
			logger.debug(getHandlerPhase(), "hiveConfs from handler properties=\"{}\"", hiveConfigurations);
		}

		if (PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
			logger.debug(getHandlerPhase(), "found hive-conf in src-desc properties");
			hiveConfigurations
					.putAll(PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF));
		}
		logger.debug(getHandlerPhase(), "hiveConfs=\"{}\"", hiveConfigurations);

	}

	@Override
	protected void initRecordToProcess(RuntimeInfo runtimeInfo) throws HandlerException {
		Map<String, String> runtimeProperty = runtimeInfo.getProperties();
		final String hiveConfDate = runtimeProperty.get("hiveConfDate");
		String outputDirectory = runtimeProperty.get("hiveConfDirectory");
		final String hiveQuery = runtimeProperty.get("hiveQuery");

		inputDescriptor = new HiveReaderDescriptor(getEntityName(), hiveConfDate, outputDirectory, hiveQuery);
		logger.debug(getHandlerPhase(), "\"initialized descriptor\" getInputDescriptorString={}",
				inputDescriptor.getInputDescriptorString());

		hiveConfigurations.put("DIRECTORY", outputDirectory);
		hiveConfigurations.put("DATE", hiveConfDate);
		// setHdfsOutputDirectory();
		try {
			setupConnection();
		} catch (ClassNotFoundException | SQLException | IOException e) {
			throw new HandlerException("unable to process", e);
		}
		// return inputDescriptor;
	}

	protected boolean findAndAddRuntimeInfoRecords() throws RuntimeInfoStoreException {
		long now = System.currentTimeMillis();
		if (hiveConfDateTime == 0) {// this is the first time

			hiveConfDateTime = now - getGoBackDays() * MILLS_IN_A_DAY;
			logger.info(getHandlerPhase(),
					"_message=\"first run, set hiveConfDateTime done\" hiveConfDateTime={} hiveConfDate={}",
					hiveConfDateTime, getHiveConfDate());
			// now - goBackDays * MILLS_IN_A_DAY;
		} else if (now - hiveConfDateTime > intervalInMillis) {
			hiveConfDateTime = hiveConfDateTime + intervalInMillis;
			logger.info(getHandlerPhase(),
					"_message=\"time to set hiveConfDateTime.\" now={} hiveConfDateTime={} intervalInMillis={} hiveConfDate={}",
					now, hiveConfDateTime, intervalInMillis, getHiveConfDate());
		} else {
			logger.info("/", "now={} hiveConfDateTime={} intervalInMillis={}", now, hiveConfDateTime, intervalInMillis);
			return false;
		}
		setHdfsOutputDirectory();
		final HiveReaderDescriptor descriptor = new HiveReaderDescriptor(getEntityName(), getHiveConfDate(),
				outputDirectory, getHiveQuery());
		Map<String, String> properties = new HashMap<>();
		properties.put("hiveConfDate", getHiveConfDate());
		properties.put("hiveConfDirectory", outputDirectory);
		properties.put("hiveQuery", getHiveQuery());

		queueRuntimeInfo(getRuntimeInfoStore(), getEntityName(), descriptor.getInputDescriptorString(), properties);

		return true;
	}

	private String setHdfsOutputDirectory() {
		// String outputDirectory = null;
		if (!getBaseOutputDirectory().endsWith(FORWARD_SLASH))
			outputDirectory = getBaseOutputDirectory() + FORWARD_SLASH;
		else
			outputDirectory = getBaseOutputDirectory();

		final String hiveConfDate = getHiveConfDate();
		final String hdfsOutputPathDate = hdfsOutputPathDtf.print(hiveConfDateTime);
		outputDirectory = outputDirectory + hdfsOutputPathDate + FORWARD_SLASH + getEntityName();

		logger.debug(getHandlerPhase(), "hiveConfDate={} hdfsOutputPathDate={} outputDirectory=\"{}\"", hiveConfDate,
				hdfsOutputPathDate, outputDirectory);

		hiveConfigurations.put("DIRECTORY", outputDirectory);
		hiveConfigurations.put("DATE", hiveConfDate);
		return outputDirectory;
	}

	/**
	 * Descriptor: entityName, date, outputDirectory, hiveQuery
	 * 
	 * tab, 20160724, dir, INSERT OVERWRITE DIRECTORY '${hiveconf:DIRECTORY}'
	 * SELECT tab.* FROM tab JOIN tab ON tab.fld = tab.fld AND tab.fld = tab.fld
	 * WHERE tab.fld IN (1,2,3,4) AND tab.fld > '${hiveconf:DATE}' DISTRIBUTE BY
	 * RAND()
	 * 
	 * 
	 */

	@Override
	public Status doProcess() throws HandlerException {

		logger.debug(getHandlerPhase(), "_messagge=\"entering doProcess\" invocation_count={}", getInvocationCount());
		try {
			ActionEvent outputEvent = new ActionEvent();
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, getEntityName());
			// outputEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH,
			// inputDescriptor.getHiveConfDirectory());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, outputDirectory);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.HIVE_QUERY, getHiveQuery());

			if (inputDescriptor == null) {
				logger.info(getHandlerPhase(),
						"will return READY, so that next handler can process the pending records");
			} else {
				logger.info(getHandlerPhase(), "\"found inputDescriptor\" inputDescriptor={}", inputDescriptor);

				final Statement stmt = connection.createStatement();
				runHiveConfs(stmt);

				String jobName = "bigdime-dw" + "." + getEntityName() + "." + ProcessHelper.getInstance().getProcessId()
						+ "." + getJobDtf().print(System.currentTimeMillis());
				logger.info(getHandlerPhase(), "hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={}", getHiveQuery(),
						hiveConfigurations, jobName);
				outputEvent.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_NAME, jobName);

				stmt.execute("set mapred.job.name=" + jobName);
				stmt.execute(getHiveQuery());// no resultset is returned
				boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
						inputDescriptor.getInputDescriptorString(),
						io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.PENDING, outputEvent.getHeaders());
				logger.info(getHandlerPhase(), "updatedRuntime={}", updatedRuntime);
			}
			getHandlerContext().createSingleItemEventList(outputEvent);
			logger.info(getHandlerPhase(), "_message=\"completed process\" headers=\"{}\"", outputEvent.getHeaders());
		} catch (final Exception e) {
			throw new HandlerException("unable to process", e);
		} finally {
			closeConnection();
		}

		return Status.READY;
	}

	private String getHiveConfDate() {
		final String hiveConfDate = getHiveQueryDtf().print(hiveConfDateTime);
		return hiveConfDate;
	}

	private void setupConnection() throws SQLException, IOException, ClassNotFoundException {
		if (connection == null) {
			if (getAuthOption() == HDFS_AUTH_OPTION.KERBEROS) {
				connection = hiveJdbcConnectionFactory.getConnectionWithKerberosAuthentication(getDriverClassName(),
						getJdbcUrl(), getUserName(), getPassword(), hiveConfigurations);
				logger.debug(getHandlerPhase(), "_message=\"connected to db\"");
			} else if (getAuthOption() == HDFS_AUTH_OPTION.PASSWORD) {
				connection = hiveJdbcConnectionFactory.getConnection(getDriverClassName(), getJdbcUrl(), getUserName(),
						getPassword(), hiveConfigurations);
				logger.debug(getHandlerPhase(), "_message=\"connected to db\"");
			}
		}
	}

	private void runHiveConfs(final Statement stmt) throws SQLException {
		if (hiveConfigurations != null) {
			for (final String prop : hiveConfigurations.keySet()) {
				String sql = "set " + prop + " = " + hiveConfigurations.get(prop);
				logger.debug(getHandlerPhase(), "running sql to set hiveconf: \"{}\"", sql);
				stmt.execute(sql);
			}
		}
	}

	private void closeConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (Exception e) {
			logger.warn(getHandlerPhase(), "_message=\"error while trying to close the connection\"", e);
		} finally {
			connection = null;
		}
	}

	public String getJdbcUrl() {
		return handlerConfig.getJdbcUrl();
	}

	public String getDriverClassName() {
		return handlerConfig.getDriverClassName();
	}

	public HDFS_AUTH_OPTION getAuthOption() {
		return handlerConfig.getAuthOption();
	}

	public String getUserName() {
		return handlerConfig.getUserName();
	}

	public String getBaseOutputDirectory() {
		return handlerConfig.getBaseOutputDirectory();
	}

	public String getEntityName() {
		return handlerConfig.getEntityName();
	}

	public String getHiveQuery() {
		return handlerConfig.getHiveQuery();
	}

	public Map<String, String> getHiveConfigurations() {
		return hiveConfigurations;
	}

	public DateTimeFormatter getJobDtf() {
		return jobDtf;
	}

	public DateTimeFormatter getHiveQueryDtf() {
		return hiveQueryDtf;
	}

	public DateTimeFormatter getHdfsOutputPathDtf() {
		return hdfsOutputPathDtf;
	}

	public int getGoBackDays() {
		return handlerConfig.getGoBackDays();
	}

	public HiveJdbcConnectionFactory getHiveJdbcConnectionFactory() {
		return hiveJdbcConnectionFactory;
	}

	public Connection getConnection() {
		return connection;
	}

	public HiveReaderDescriptor getInputDescriptor() {
		return inputDescriptor;
	}

	public long getHiveConfDateTime() {
		return hiveConfDateTime;
	}

	public long getIntervalInMins() {
		return intervalInMins;
	}

	public long getIntervalInMillis() {
		return intervalInMillis;
	}

	public String getPassword() {
		return handlerConfig.getPassword();
	}

	public String getOutputDirectoryPattern() {
		return handlerConfig.getOutputDirectoryPattern();
	}

	@Override
	protected String getInputDescriptorPrefix() {
		return INPUT_DESCRIPTOR_PREFIX;
	}

	@Override
	protected void setInputDescriptorToNull() {
		inputDescriptor = null;
	}

	@Override
	protected boolean isInputDescriptorNull() {
		return inputDescriptor == null;
	}
}
