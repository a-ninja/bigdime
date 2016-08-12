package io.bigdime.handler.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
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
import io.bigdime.core.commons.DateNaturalLanguageExpressionParser;
import io.bigdime.core.commons.ProcessHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;
import io.bigdime.libs.hdfs.jdbc.HiveJdbcConnectionFactory;
import io.bigdime.libs.hdfs.job.YarnJobHelper;

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
	private static final long DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS = TimeUnit.MINUTES.toSeconds(5);
	private static final int DEFAULT_MAX_RETRIES = 5;

	private static long sleepBetweenRetriesMillis;
	private static int maxRetries;

	final private String DEFAULT_MIN_GO_BACK = "1 day";

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

		long sleep = PropertyHelper.getLongProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.SLEEP_BETWEEN_RETRY_SECONDS, DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS);
		if (sleep == 0)
			sleep = DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS;
		sleepBetweenRetriesMillis = TimeUnit.SECONDS.toMillis(sleep);

		maxRetries = PropertyHelper.getIntProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.MAX_RETRIES,
				DEFAULT_MAX_RETRIES);

		if (maxRetries < 0)
			maxRetries = 0;
		logger.info(getHandlerPhase(),
				"entityName=\"{}\" hiveQuery=\"{}\" goBackDays={} sleepBetweenRetriesMillis={} maxRetries={}",
				entityName, hiveQuery, goBackDays, sleepBetweenRetriesMillis, maxRetries);

		Preconditions.checkArgument(goBackDays >= 0,
				HiveJdbcReaderHandlerConstants.GO_BACK_DAYS + " has to be a non-negative value.");

		String minGoBackExpression = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.MIN_GO_BACK, DEFAULT_MIN_GO_BACK);
		long minGoBackMillis = DateNaturalLanguageExpressionParser.toMillis(minGoBackExpression);

		Preconditions.checkArgument(goBackDays * MILLS_IN_A_DAY >= minGoBackMillis,
				"\"go-back-days\" must be more than \"min-go-back\"");

		for (String key : srcDescValueMap.keySet()) {
			logger.info(getHandlerPhase(), "srcDesc-key=\"{}\" srcDesc-value=\"{}\"", key, srcDescValueMap.get(key));
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
				"jdbcUrl=\"{}\" driverClassName=\"{}\" authChoice={} authOption={} userName=\"{}\" password=\"****\" baseOutputDirectory={} outputDirectoryPattern={}",
				jdbcUrl, driverClassName, authChoice, authOption, userName, baseOutputDirectory,
				outputDirectoryPattern);
		handlerConfig.setAuthOption(authOption);
		handlerConfig.setBaseOutputDirectory(baseOutputDirectory);
		handlerConfig.setDriverClassName(driverClassName);
		handlerConfig.setEntityName(entityName);
		handlerConfig.setGoBackDays(goBackDays);
		handlerConfig.setHiveQuery(hiveQuery);
		handlerConfig.setJdbcUrl(jdbcUrl);
		handlerConfig.setPassword(password);
		handlerConfig.setUserName(userName);
		handlerConfig.setMinGoBack(minGoBackMillis);
		logger.info(getHandlerPhase(), "handlerConfig={}", handlerConfig);
	}

	@SuppressWarnings("unchecked")
	private void setHiveConfigurations(Map<String, Object> srcDescValueMap) {
		if (getPropertyMap().get(HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
			logger.info(getHandlerPhase(), "found hive-conf in handler properties");
			hiveConfigurations
					.putAll(PropertyHelper.getMapProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.HIVE_CONF));
			logger.info(getHandlerPhase(), "hiveConfs from handler properties=\"{}\"", hiveConfigurations);
		}

		if (PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
			logger.info(getHandlerPhase(), "found hive-conf in src-desc properties");
			hiveConfigurations
					.putAll(PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF));
		}
		logger.info(getHandlerPhase(), "hiveConfs=\"{}\"", hiveConfigurations);

	}

	private org.apache.hadoop.conf.Configuration conf;

	protected void initClass() throws RuntimeInfoStoreException {
		if (isFirstRun()) {
			logger.info(getHandlerPhase(), "initializing yarn JobClient instance");
			try {
				if (getPropertyMap().get(HiveJdbcReaderHandlerConstants.YARN_CONF) != null) {
					conf = new org.apache.hadoop.conf.Configuration();

					@SuppressWarnings("unchecked")
					Map<String, String> yarnConfs = PropertyHelper.getMapProperty(getPropertyMap(),
							HiveJdbcReaderHandlerConstants.YARN_CONF);
					logger.debug(getHandlerPhase(), "yarn_confs={}", yarnConfs);
					for (Entry<String, String> entry : yarnConfs.entrySet()) {
						conf.set(entry.getKey(), entry.getValue());
					}
					UserGroupInformation.setConfiguration(conf);
					UserGroupInformation.loginUserFromKeytab(handlerConfig.getUserName(), handlerConfig.getPassword());
					// yarnJobHelper = new YarnJobHelper();
					logger.info(getHandlerPhase(), "initialized yarn JobClient instance");
				}
			} catch (Throwable e) {
				logger.warn("jobclient", "error during yarn JobClient initialization", e);
			}
		}
	}

	@Override
	protected void initRecordToProcess(RuntimeInfo runtimeInfo) throws HandlerException {
		Map<String, String> runtimeProperty = runtimeInfo.getProperties();
		final String hiveConfDate = runtimeProperty.get("hiveConfDate");
		outputDirectory = runtimeProperty.get("hiveConfDirectory");
		final String hiveQuery = runtimeProperty.get("hiveQuery");
		final String jobName = runtimeProperty.get(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_NAME);

		inputDescriptor = new HiveReaderDescriptor(getEntityName(), hiveConfDate, outputDirectory, hiveQuery);
		inputDescriptor.setJobName(jobName);
		logger.debug(getHandlerPhase(), "\"initialized descriptor\" getInputDescriptorString={} jobName={}",
				inputDescriptor.getInputDescriptorString(), jobName);

		hiveConfigurations.put("DIRECTORY", outputDirectory);
		hiveConfigurations.put("DATE", hiveConfDate);
		// setHdfsOutputDirectory();
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
			// } else if (now - hiveConfDateTime > intervalInMillis) {
		} else if (now - hiveConfDateTime > handlerConfig.getMinGoBack()) {
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

		String jobName = "bigdime" + "." + getEntityName() + "." + ProcessHelper.getInstance().getProcessId() + "."
				+ getJobDtf().print(System.currentTimeMillis());
		logger.info(getHandlerPhase(),
				"findAndAddRuntimeInfoRecords: hiveQuery=\"{}\" hiveConfigurations=\"{}\" outputDirectory={} jobName={}",
				getHiveQuery(), hiveConfigurations, outputDirectory, jobName);
		properties.put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_NAME, jobName);

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
	 * 
	 * If all is good started record in RTI start the job
	 * 
	 * 
	 * Descriptor: entityName, date, outputDirectory, hiveQuery
	 * 
	 * tab, 20160724, dir, INSERT OVERWRITE DIRECTORY '${hiveconf:DIRECTORY}'
	 * SELECT tab.* FROM tab JOIN tab ON tab.fld = tab.fld AND tab.fld = tab.fld
	 * WHERE tab.fld IN (1,2,3,4) AND tab.fld > '${hiveconf:DATE}' DISTRIBUTE BY
	 * RAND()
	 * 
	 * 
	 */

	protected Status processNullDescriptor() {
		if (isFirstRun()) {
			logger.info(getHandlerPhase(),
					"_message=\"will return CALLBACK, so that next handler can process the pending records\" entityName={}",
					getEntityName());
			return Status.CALLBACK;
		} else {
			logger.info(getHandlerPhase(),
					"_message=\"will return READY, so that next handler can process the pending records\" entityName={}",
					getEntityName());
			return Status.READY;
		}
	}

	@Override
	public Status doProcess() throws HandlerException {
		logger.info(getHandlerPhase(), "_messagge=\"entering doProcess\" invocation_count={}", getInvocationCount());
		Status returnStatus = Status.READY;
		try {
			ActionEvent outputEvent = new ActionEvent();
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, getEntityName());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, outputDirectory);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.HIVE_QUERY, getHiveQuery());

			String jobName = null;
			if (inputDescriptor == null) {
				returnStatus = processNullDescriptor();
			} else {
				logger.info(getHandlerPhase(), "\"found inputDescriptor\" inputDescriptor={}", inputDescriptor);

				jobName = inputDescriptor.getJobName();
				returnStatus = processPreviouslySubmittedJobInfo(jobName, outputEvent);
				if (returnStatus == null) {
					runWithRetries(outputEvent, jobName);
					boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
							inputDescriptor.getInputDescriptorString(),
							io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.PENDING, outputEvent.getHeaders());
					logger.info(getHandlerPhase(), "updatedRuntime={}", updatedRuntime);

					returnStatus = Status.READY;
				}
			}
			getHandlerContext().createSingleItemEventList(outputEvent);
			logger.info(getHandlerPhase(), "_message=\"completed process\" headers=\"{}\" returnStatus={} jobName={}",
					outputEvent.getHeaders(), returnStatus, jobName);
		} catch (final Exception e) {
			throw new HandlerException("unable to process" + e.getMessage(), e);
		}
		return returnStatus;
	}

	private String getHiveConfDate() {
		final String hiveConfDate = getHiveQueryDtf().print(hiveConfDateTime);
		return hiveConfDate;
	}

	private Connection setupConnection() throws SQLException, IOException, ClassNotFoundException {
		Connection connection = null;
		if (connection == null) {
			if (getAuthOption() == HDFS_AUTH_OPTION.KERBEROS) {
				connection = hiveJdbcConnectionFactory.getConnectionWithKerberosAuthentication(getDriverClassName(),
						getJdbcUrl(), getUserName(), getPassword(), hiveConfigurations);
				logger.info(getHandlerPhase(), "connected to db using kerberos");
			} else if (getAuthOption() == HDFS_AUTH_OPTION.PASSWORD) {
				connection = hiveJdbcConnectionFactory.getConnection(getDriverClassName(), getJdbcUrl(), getUserName(),
						getPassword(), hiveConfigurations);
				logger.info(getHandlerPhase(), "connected to db using password");
			}
		}
		return connection;
	}

	private void runHiveConfs(final Statement stmt) throws SQLException {
		if (hiveConfigurations != null) {
			for (final String prop : hiveConfigurations.keySet()) {
				String sql = "set " + prop + "=" + hiveConfigurations.get(prop);
				logger.debug(getHandlerPhase(), "running sql to set hiveconf: \"{}\"", sql);
				stmt.execute(sql);
			}
		}
	}

	private void closeConnection(final Connection connection) {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (Exception e) {
			logger.warn(getHandlerPhase(), "_message=\"error while trying to close the connection\"", e);
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

	private ExecutorService pool;
	FutureTask<Void> futureTask;

	private void submitJob(final String jobName) {
		pool = Executors.newSingleThreadExecutor();

		futureTask = new FutureTask<>(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				Connection connection = setupConnection();
				try {
					final Statement stmt = connection.createStatement();
					stmt.execute("set mapred.job.name=" + jobName);
					runHiveConfs(stmt);
					stmt.execute(getHiveQuery());// no resultset is
					logger.info(getHandlerPhase(), "sql statement ran");
					return null;
				} finally {
					closeConnection(connection);
				}

			}
		});
		logger.info(getHandlerPhase(), "initializing executor");
		pool.execute(futureTask);
	}

	private boolean updateRunningStatus(ActionEvent event, JobStatus jobStatus) throws RuntimeInfoStoreException {
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_ID,
				jobStatus.getJobID().toString());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_START_TIME,
				"" + jobStatus.getStartTime());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FINISH_TIME,
				"" + jobStatus.getFinishTime());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FAILURE_INFO,
				jobStatus.getFailureInfo());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_INTERIM_STATUS,
				JobStatus.getJobRunState(jobStatus.getRunState()));

		boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
				inputDescriptor.getInputDescriptorString(), io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.STARTED,
				event.getHeaders());

		return updatedRuntime;
	}

	private boolean updateSuccessfulStatus(ActionEvent event, JobStatus jobStatus) throws RuntimeInfoStoreException {
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_ID,
				jobStatus.getJobID().toString());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_START_TIME,
				"" + jobStatus.getStartTime());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FINISH_TIME,
				"" + jobStatus.getFinishTime());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FAILURE_INFO,
				jobStatus.getFailureInfo());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_COMPLETION_STATUS,
				JobStatus.getJobRunState(jobStatus.getRunState()));

		boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
				inputDescriptor.getInputDescriptorString(), io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.PENDING,
				event.getHeaders());

		return updatedRuntime;
	}

	private boolean updateFailedStatus(ActionEvent event, JobStatus jobStatus) throws RuntimeInfoStoreException {

		RuntimeInfo rtiRecord = getRuntimeInfoStore().get(AdaptorConfig.getInstance().getName(), getEntityName(),
				inputDescriptor.getInputDescriptorString());

		String origJobId = rtiRecord.getProperties()
				.get(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_ORIG_JOB_ID);

		if (StringHelper.isBlank(origJobId)) {
			origJobId = origJobId + "," + jobStatus.getJobID().toString();
		}
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_ORIG_JOB_ID, origJobId);

		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_ID,
				jobStatus.getJobID().toString());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_START_TIME,
				"" + jobStatus.getStartTime());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FINISH_TIME,
				"" + jobStatus.getFinishTime());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FAILURE_INFO,
				jobStatus.getFailureInfo());
		event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_COMPLETION_STATUS,
				JobStatus.getJobRunState(jobStatus.getRunState()));

		boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
				inputDescriptor.getInputDescriptorString(), io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.FAILED,
				event.getHeaders());

		return updatedRuntime;
	}

	private boolean jobSucceeded(String jobName, ActionEvent outputEvent) throws RuntimeInfoStoreException {
		try {
			YarnJobHelper yarnJobHelper = new YarnJobHelper();
			JobStatus completedJobStatus = yarnJobHelper.getStatusForCompletedJob(jobName, conf);
			boolean updatedRuntime = false;
			if (completedJobStatus != null && completedJobStatus.getRunState() == JobStatus.SUCCEEDED) {
				updatedRuntime = updateSuccessfulStatus(outputEvent, completedJobStatus);
			} else {
				updatedRuntime = updateFailedStatus(outputEvent, completedJobStatus);
			}
			logger.info(getHandlerPhase(),
					"_message=\"after job completion\" updatedRuntime={} jobID={} jobName={} runState={} runState={}",
					updatedRuntime, completedJobStatus.getJobID().toString(), jobName, completedJobStatus.getRunState(),
					JobStatus.getJobRunState(completedJobStatus.getRunState()));
			return completedJobStatus.getRunState() == JobStatus.SUCCEEDED;

		} catch (final IOException ex) {
			logger.warn(getHandlerPhase(), "_message=\"jobSucceeded:unable to get the job status\" jobName={}", jobName,
					ex);
			return false;
		}
	}

	private Status processPreviouslySubmittedJobInfo(String jobName, ActionEvent outputEvent)
			throws RuntimeInfoStoreException {
		JobStatus jobStatus = null;
		int runState = -1;
		Status returnStatus = null;
		try {
			YarnJobHelper yarnJobHelper = new YarnJobHelper();
			jobStatus = yarnJobHelper.getPositiveStatusForJob(jobName, conf);
			if (jobStatus != null)
				runState = jobStatus.getRunState();
		} catch (IOException ex) {
			logger.warn(getHandlerPhase(),
					"_message=\"processPreviouslySubmittedJobInfo: unable to get the job status\" jobName={}", jobName,
					ex);
		}

		if (runState == JobStatus.RUNNING || runState == JobStatus.PREP) {
			logger.info(getHandlerPhase(),
					"_message=\"job is already running. will return callback\" jobID={} hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={}",
					jobStatus.getJobID().toString(), getHiveQuery(), hiveConfigurations, jobName);
			boolean updatedRuntime = updateRunningStatus(outputEvent, jobStatus);
			logger.info(getHandlerPhase(), "updatedRuntime={}", updatedRuntime);
			try {
				logger.info(getHandlerPhase(), "sleeping before callback");
				Thread.sleep(sleepBetweenRetriesMillis);
			} catch (Exception ex) {
				logger.info(getHandlerPhase(), "_message=\"sleep interrupted before call back\" exception={}", ex);
			}
			returnStatus = Status.CALLBACK;
		} else if (runState == JobStatus.SUCCEEDED) {
			logger.info(getHandlerPhase(),
					"_message=\"job is successfully completed.\" jobID={} hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={}",
					jobStatus.getJobID().toString(), getHiveQuery(), hiveConfigurations, jobName);
			boolean updatedRuntime = updateSuccessfulStatus(outputEvent, jobStatus);
			logger.info(getHandlerPhase(), "updatedRuntime={}", updatedRuntime);
			returnStatus = Status.READY;
		}
		return returnStatus;

	}

	private String computeJobName() {
		return "bigdime" + "." + getEntityName() + "." + ProcessHelper.getInstance().getProcessId() + "."
				+ getJobDtf().print(System.currentTimeMillis());
	}

	private void runWithRetries(ActionEvent outputEvent, String jobName)
			throws SQLException, IOException, RuntimeInfoStoreException {

		String newJobName = computeJobName();
		boolean jobRanSuccessfully = false;
		int attempts = 0;
		logger.info(getHandlerPhase(),
				"_message=\"job is going to be submitted\" hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={} newJobName={}",
				getHiveQuery(), hiveConfigurations, jobName, newJobName);
		outputEvent.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_NAME, newJobName);
		do {
			try {
				attempts++;
				submitJob(newJobName);

				logger.info(getHandlerPhase(), "_mesage=\"submitted job\" attempts={}", attempts);

				boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
						inputDescriptor.getInputDescriptorString(),
						io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.STARTED, outputEvent.getHeaders());

				logger.info(getHandlerPhase(), "_message=\"after job submission\" updatedRuntime={} jobName={}",
						updatedRuntime, newJobName);
				processStatusForNewJob(outputEvent, newJobName);
				futureTask.get();
				pool.shutdown();
				logger.info(getHandlerPhase(), "_mesage=\"job completed, future returned\" jobName={}", newJobName);
				jobRanSuccessfully = jobSucceeded(newJobName, outputEvent);
				if (jobRanSuccessfully)
					break;

			} catch (final Exception ex) {
				YarnJobHelper yarnJobHelper = new YarnJobHelper();
				JobStatus failedJobStatus = yarnJobHelper.getStatusForNewJob(newJobName, conf);

				boolean updatedRuntime = updateFailedStatus(outputEvent, failedJobStatus);
				logger.warn(getHandlerPhase(),
						"_message=\"error in running the job\" updatedRuntime={} jobName={} error={}", updatedRuntime,
						newJobName, ex.getMessage(), ex);
			}
			if (attempts <= maxRetries) {
				logger.info(getHandlerPhase(), "will sleep for 3 mins and retry");
				try {
					Thread.sleep(sleepBetweenRetriesMillis);
				} catch (Exception ex) {
					logger.info(getHandlerPhase(), "Thread interrupted", ex);
				}
			}

		} while (!jobRanSuccessfully && attempts <= maxRetries);

	}

	private void processStatusForNewJob(ActionEvent outputEvent, String jobName) throws RuntimeInfoStoreException {
		try {
			YarnJobHelper yarnJobHelper = new YarnJobHelper();
			JobStatus newJobStatus = yarnJobHelper.getStatusForNewJob(jobName, conf);
			if (newJobStatus != null) {
				boolean updatedRuntime = updateRunningStatus(outputEvent, newJobStatus);
				logger.info(getHandlerPhase(),
						"_message=\"after submitting the job\" updatedRuntime={} jobID={} jobName={} runState={} runState={}",
						updatedRuntime, newJobStatus.getJobID().toString(), jobName, newJobStatus.getRunState(),
						JobStatus.getJobRunState(newJobStatus.getRunState()));
			}
		} catch (IOException ex) {
			logger.warn(getHandlerPhase(),
					"_message=\"processStatusForNewJob:unable to get the job status\" jobName={}", jobName, ex);
		}

	}
}
