package io.bigdime.handler.hive;

import com.google.common.base.Preconditions;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.*;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.libs.hive.job.JobStatusException;
import io.bigdime.libs.hive.job.JobStatusFetcher;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;
import io.bigdime.libs.hdfs.HiveConnectionParam;
import io.bigdime.libs.hdfs.WebHdfsReader;
import io.bigdime.libs.hdfs.HiveJdbcConnectionFactory;
import io.bigdime.libs.hive.job.HiveJobSpec;
import io.bigdime.libs.hive.job.HiveJobStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * HiveReaderHandler reads data from a hive table/query and outputs each row as
 * one event.
 * <p>
 * HiveReaderHandler: Configure one source for each recurring file. Store data
 * in file. Send the output filename in the header. File reader handler to get
 * the filename from the header. SwiftWriter
 * <p>
 * src-desc : { entity-name query
 * <p>
 * "input1": { "entity-name": "tracking_events", "hive-query": "query"
 * "hive-conf": { "mapred.job.queue.name" : "queuename",
 * "mapred.output.compress" : "true", "hive.exec.compress.output" : "true",
 * "mapred.output.compression.codec" :
 * "org.apache.hadoop.io.compress.GzipCodec", "io.compression.codecs" :
 * "org.apache.hadoop.io.compress.GzipCodec", "mapred.reduce.tasks" : "500"
 * <p>
 * "schemaFileName" :"${hive_schema_file_name}" },
 * <p>
 * "input1" : "table1: table1 ", "input2" : "table2: table2"
 * <p>
 * <p>
 * }
 *
 * @author Neeraj Jain
 */
@Component
@Scope("prototype")
public final class HiveJdbcReaderHandler extends AbstractSourceHandler {
  private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HiveJdbcReaderHandler.class));
  private String outputDirectory = "";
  final Map<String, String> hiveConfigurations = new HashMap<>();
  @Autowired
  HiveJdbcConnectionFactory hiveJdbcConnectionFactory;

  @Autowired
  private WebHdfsReader webHdfsReader;

  @Autowired
  @Qualifier("hiveJobStatusFether")
  private JobStatusFetcher<HiveJobSpec, HiveJobStatus> hiveJobStatusFetcher;

  private NextRunTimeRecordLoader<Long, Long> nextRunTimeRecordLoader;
  private HiveReaderDescriptor inputDescriptor;
  private long hiveConfDateTime;

  private static final long MILLS_IN_A_DAY = 24 * 60 * 60 * 1000l;
  private long intervalInMins = 24 * 60;// default to a day
  private long intervalInMillis = intervalInMins * 60 * 1000;

  private static final String OUTPUT_DIRECTORY_DATE_FORMAT = "yyyy-MM-dd";

  private static final String INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.hive.HiveJdbcReaderHandler";
  final private int DEFAULT_GO_BACK_DAYS = 1;
  private HiveJdbcReaderHandlerConfig handlerConfig = new HiveJdbcReaderHandlerConfig();

  final DateTimeFormatter jobDtf = DateTimeFormat.forPattern("yyyyMMdd-HHmmss.SSS");
  private DateTimeFormatter hiveQueryDtf;
  final private DateTimeZone dateTimeZone = DateTimeZone.forID("America/Los_Angeles");
  private DateTimeFormatter hdfsOutputPathDtf;
  public static final String FORWARD_SLASH = "/";
  private static final long DEFAULT_SLEEP_BETWEEN_RETRIES_SECONDS = TimeUnit.MINUTES.toSeconds(5);
  private static final int DEFAULT_MAX_RETRIES = 5;
  private static final String DEFAULT_LATENCY = "0 hours";

  private static long sleepBetweenRetriesMillis;
  private static int maxRetries;
  private org.apache.hadoop.conf.Configuration conf;

  final private String DEFAULT_MIN_GO_BACK = "1 day";
  private HiveConnectionParam hiveConnectionParam = null;

  @Override
  public void build() throws AdaptorConfigurationException {
    setHandlerPhase("building HiveJdbcReaderHandler");
    super.build();
    logger.info(getHandlerPhase(), "properties={}", getPropertyMap());

    logger.info(getHandlerPhase(), "webHdfsReader={}", webHdfsReader);

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

    goBackDays = PropertyHelper.getIntPropertyFromPropertiesOrSrcDesc(properties, srcDescValueMap,
            HiveJdbcReaderHandlerConstants.GO_BACK_DAYS, DEFAULT_GO_BACK_DAYS);

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

    String minGoBackExpression = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(properties,
            srcDescValueMap, HiveJdbcReaderHandlerConstants.MIN_GO_BACK, DEFAULT_MIN_GO_BACK);

    long minGoBackMillis = DateNaturalLanguageExpressionParser.toMillis(minGoBackExpression);

    Preconditions.checkArgument(goBackDays * MILLS_IN_A_DAY >= minGoBackMillis,
            "\"go-back-days\"(" + goBackDays * MILLS_IN_A_DAY + ") must be more than \"min-go-back\"(" + minGoBackMillis + ")");

    String latencyExpression = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(getPropertyMap(),
            srcDescValueMap, HiveJdbcReaderHandlerConstants.LATENCY, DEFAULT_LATENCY);
    long latencyInMillis = DateNaturalLanguageExpressionParser.toMillis(latencyExpression);

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

    hiveConnectionParam = new HiveConnectionParam(authOption, driverClassName, jdbcUrl, userName, password, hiveConfigurations);
    baseOutputDirectory = PropertyHelper.getStringProperty(getPropertyMap(),
            HiveJdbcReaderHandlerConstants.BASE_OUTPUT_DIRECTORY, "/");
    String outputDirectoryPattern = PropertyHelper.getStringProperty(getPropertyMap(),
            HiveJdbcReaderHandlerConstants.OUTPUT_DIRECTORY_DATE_FORMAT, OUTPUT_DIRECTORY_DATE_FORMAT);
    hdfsOutputPathDtf = DateTimeFormat.forPattern(outputDirectoryPattern);

    String hiveQueryDateFormat = PropertyHelper.getStringProperty(getPropertyMap(),
            HiveJdbcReaderHandlerConstants.HIVE_QUERY_DATE_FORMAT, "yyyy-MM-dd");

    hiveQueryDtf = DateTimeFormat.forPattern(hiveQueryDateFormat);

    String touchFile = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(properties, srcDescValueMap,
            HiveJdbcReaderHandlerConstants.TOUCH_FILE, null);

    logger.info(getHandlerPhase(),
            "jdbcUrl=\"{}\" driverClassName=\"{}\" authChoice={} authOption={} userName=\"{}\" password=\"****\" baseOutputDirectory={} outputDirectoryPattern={} hiveQueryDateFormat={} touchFile={}",
            jdbcUrl, driverClassName, authChoice, authOption, userName, baseOutputDirectory, outputDirectoryPattern,
            hiveQueryDateFormat, touchFile);
    handlerConfig.setBaseOutputDirectory(baseOutputDirectory);
    handlerConfig.setEntityName(entityName);
    handlerConfig.setGoBackDays(goBackDays);
    handlerConfig.setHiveQuery(hiveQuery);
    handlerConfig.setMinGoBack(minGoBackMillis);
    handlerConfig.setLatency(latencyInMillis);
    handlerConfig.setTouchFile(touchFile);

    if (touchFile == null) {
      nextRunTimeRecordLoader = new LatencyNextRunTimeRecordLoader(new TouchFileLookupConfig(handlerConfig), getPropertyMap());
    } else {
      nextRunTimeRecordLoader = new TouchFileNextRunTimeRecordLoader(webHdfsReader, new TouchFileLookupConfig(handlerConfig), getPropertyMap());
    }

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

  protected void initClass() throws RuntimeInfoStoreException {
    if (isFirstRun()) {
      logger.info(getHandlerPhase(), "initializing yarn JobClient instance");
      try {
        if (getPropertyMap().get(HiveJdbcReaderHandlerConstants.YARN_CONF) != null) {
          conf = new org.apache.hadoop.conf.Configuration();

          @SuppressWarnings("unchecked")
          Map<String, String> yarnConfs = PropertyHelper.getMapProperty(getPropertyMap(),
                  HiveJdbcReaderHandlerConstants.YARN_CONF);
          logger.info(getHandlerPhase(), "yarn_confs={}", yarnConfs);
          for (Entry<String, String> entry : yarnConfs.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
          }
          UserGroupInformation.setConfiguration(conf);
          UserGroupInformation.loginUserFromKeytab(hiveConnectionParam.userName(), hiveConnectionParam.password());
          logger.info(getHandlerPhase(), "initialized yarn JobClient instance");
        }
      } catch (Exception e) {
        logger.warn("jobclient", "error during yarn JobClient initialization", e);
      }
    }
  }

  @Override
  protected void initRecordToProcess(RuntimeInfo runtimeInfo) throws HandlerException {
    Map<String, String> runtimeProperty = runtimeInfo.getProperties();
    final String hiveConfDate = runtimeProperty.get("hiveConfDate");

    DateTime parsed = getHiveQueryDtf().withZone(dateTimeZone).parseDateTime(hiveConfDate);
    hiveConfDateTime = parsed.getMillis();
    logger.info(getHandlerPhase(),
            "_message=\"setting hiveConfDateTime in initRecordToProcess\" hiveConfDateTime={}", hiveConfDateTime);

    outputDirectory = runtimeProperty.get("hiveConfDirectory");
    final String hiveQuery = runtimeProperty.get("hiveQuery");
    final String jobName = runtimeProperty.get(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_NAME);

    inputDescriptor = new HiveReaderDescriptor(getEntityName(), hiveConfDate, outputDirectory, hiveQuery);
    inputDescriptor.setJobName(jobName);
    logger.debug(getHandlerPhase(), "\"initialized descriptor\" getInputDescriptorString={} jobName={}",
            inputDescriptor.getInputDescriptorString(), jobName);

    hiveConfigurations.put("DIRECTORY", outputDirectory);
    hiveConfigurations.put("DATE", hiveConfDate);
  }

  protected boolean findAndAddRuntimeInfoRecords() throws RuntimeInfoStoreException {
    long nextRunDateTime = nextRunTimeRecordLoader.getRecords(hiveConfDateTime);
    if (nextRunDateTime == 0) {
      logger.info("nothing to do", "hiveConfDateTime={}", hiveConfDateTime);
      return false;
    } else {
      hiveConfDateTime = nextRunDateTime;
    }
    setHdfsOutputDirectory();
    final HiveReaderDescriptor descriptor = new HiveReaderDescriptor(getEntityName(), getHiveConfDate(),
            outputDirectory, getHiveQuery());
    Map<String, String> properties = new HashMap<>();
    properties.put("hiveConfDate", getHiveConfDate());
    properties.put("hiveConfDirectory", outputDirectory);
    properties.put("hiveQuery", getHiveQuery());

    String jobName = computeJobName();
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

    logger.info(getHandlerPhase(), "hiveConfDate={} hdfsOutputPathDate={} outputDirectory=\"{}\"", hiveConfDate,
            hdfsOutputPathDate, outputDirectory);

    hiveConfigurations.put("DIRECTORY", outputDirectory);
    hiveConfigurations.put("DATE", hiveConfDate);
    return outputDirectory;
  }

  /**
   * If all is good started record in RTI start the job
   * <p>
   * <p>
   * Descriptor: entityName, date, outputDirectory, hiveQuery
   * <p>
   * tab, 20160724, dir, INSERT OVERWRITE DIRECTORY '${hiveconf:DIRECTORY}'
   * SELECT tab.* FROM tab JOIN tab ON tab.fld = tab.fld AND tab.fld = tab.fld
   * WHERE tab.fld IN (1,2,3,4) AND tab.fld > '${hiveconf:DATE}' DISTRIBUTE BY
   * RAND()
   */

  @Override
  public Status doProcess() throws HandlerException {
    logger.info(getHandlerPhase(), "_messagge=\"entering doProcess\" invocation_count={}", getInvocationCount());
    Status returnStatus;
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
          RuntimeInfo rti = getRuntimeInfoStore().get(AdaptorConfig.getInstance().getName(), getEntityName(),
                  inputDescriptor.getInputDescriptorString());
          outputEvent.getHeaders().put(ActionEventHeaderConstants.PARENT_RUNTIME_ID, rti.getRuntimeId());
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
      throw new HandlerException("unable to process:" + e.getMessage(), e);
    }
    return returnStatus;
  }

  /**
   * If there was nothing new to run.
   *
   * @return
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

  private String getHiveConfDate() {
    final String hiveConfDate = getHiveQueryDtf().print(hiveConfDateTime);
    DateTime parsed = getHiveQueryDtf().withZone(dateTimeZone).parseDateTime(hiveConfDate);
    hiveConfDateTime = parsed.getMillis();
    logger.info(getHandlerPhase(), "_message=\"setting hiveConfDateTime\" hiveConfDateTime={}", hiveConfDateTime);
    return hiveConfDate;
  }

  private Connection setupConnection() throws SQLException, IOException, ClassNotFoundException {
    return hiveJdbcConnectionFactory.getConnection(hiveConnectionParam);
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
    return hiveConnectionParam.jdbcUrl();
  }

  public String getDriverClassName() {
    return hiveConnectionParam.driverClassName();
  }

  public HDFS_AUTH_OPTION getAuthOption() {
    return hiveConnectionParam.hdfsAuthOption();
  }

  public String getUserName() {
    return hiveConnectionParam.userName();
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
          stmt.execute("set mapreduce.job.name=" + jobName);
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

  private void populateHeaders(ActionEvent event, JobStatus jobStatus, String statusHeaderName, String status) {
    event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_ID,
            jobStatus.getJobID().toString());
    event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_START_TIME,
            "" + jobStatus.getStartTime());
    event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FINISH_TIME,
            "" + jobStatus.getFinishTime());
    event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_FAILURE_INFO,
            jobStatus.getFailureInfo());

    event.getHeaders().put(statusHeaderName, status);

  }

  private Status processPreviouslySubmittedJobInfo(String jobName, ActionEvent outputEvent)
          throws RuntimeInfoStoreException {
    JobStatus jobStatus = null;
    JobStatus.State state = null;
    Status returnStatus = null;
    try {
      HiveJobSpec hiveJobSpec = new HiveJobSpec(jobName, inputDescriptor.getHiveConfDirectory());
      HiveJobStatus hiveJobStatus = hiveJobStatusFetcher.getStatusForJob(hiveJobSpec);
      if (hiveJobStatus != null) {
        state = hiveJobStatus.getOverallStatus().getState();
        jobStatus = hiveJobStatus.getOverallStatus();
        logger.info(getHandlerPhase(), "processPreviouslySubmittedJobInfo: found a valid Status. state={}",
                state);
      }
    } catch (Exception ex) {
      logger.warn(getHandlerPhase(),
              "_message=\"processPreviouslySubmittedJobInfo: unable to get the job status\" jobName={}", jobName,
              ex);
    }

    if (state == JobStatus.State.RUNNING || state == JobStatus.State.PREP) {
      logger.info(getHandlerPhase(),
              "_message=\"job is already running. will return callback\" jobID={} hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={}",
              jobStatus.getJobID().toString(), getHiveQuery(), hiveConfigurations, jobName);
      boolean updatedRuntime = recordRunningStatus(outputEvent, jobStatus);
      logger.info(getHandlerPhase(), "updatedRuntime={}", updatedRuntime);
      try {
        logger.info(getHandlerPhase(), "sleeping before callback");
        Thread.sleep(sleepBetweenRetriesMillis);
      } catch (Exception ex) {
        logger.info(getHandlerPhase(), "_message=\"sleep interrupted before call back\" exception={}", ex);
      }
      returnStatus = Status.CALLBACK; // callback since there could be
      // other pending tasks that other
      // handlers want to process
    } else if (state == JobStatus.State.SUCCEEDED) {
      logger.info(getHandlerPhase(),
              "_message=\"job is successfully completed.\" jobID={} hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={}",
              jobStatus.getJobID().toString(), getHiveQuery(), hiveConfigurations, jobName);
      RuntimeInfo rti = getRuntimeInfoStore().get(AdaptorConfig.getInstance().getName(), getEntityName(),
              inputDescriptor.getInputDescriptorString());
      outputEvent.getHeaders().put(ActionEventHeaderConstants.PARENT_RUNTIME_ID, rti.getRuntimeId());

      boolean updatedRuntime = recordSuccessfulStatus(outputEvent, jobStatus);
      logger.info(getHandlerPhase(), "updatedRuntime={}", updatedRuntime);
      returnStatus = Status.READY;
    }
    logger.info(getHandlerPhase(), "_message=\"returning from processPreviouslySubmittedJobInfo.\" returnStatus={}",
            returnStatus);
    return returnStatus;
  }

  private String computeJobName() {
    return "bigdime" + "." + getEntityName() + "." + getHiveConfDate() + "."
            + ProcessHelper.getInstance().getProcessId() + "." + getJobDtf().print(System.currentTimeMillis());
  }

  private void runWithRetries(ActionEvent outputEvent, String jobName)
          throws SQLException, IOException, RuntimeInfoStoreException, JobStatusException {

    String newJobName = computeJobName();
    boolean jobRanSuccessfully = false;
    int attempts = 0;
    logger.info(getHandlerPhase(),
            "_message=\"job is going to be submitted\" hiveQuery=\"{}\" hiveConfigurations=\"{}\" jobName={} newJobName={}",
            getHiveQuery(), hiveConfigurations, jobName, newJobName);
    outputEvent.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_NAME, newJobName);
    boolean jobStarted = false;
    do {
      try {
        if (attempts > 0) {
          jobStarted = jobStartedSuccessfully(newJobName, outputEvent);
        }
        if (jobStarted) {
          break;
        }
        attempts++;
        submitJob(newJobName);

        logger.info(getHandlerPhase(), "_mesage=\"submitted job\" attempts={}", attempts);

        boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
                inputDescriptor.getInputDescriptorString(),
                io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.STARTED, outputEvent.getHeaders());

        logger.info(getHandlerPhase(), "_message=\"after job submission\" updatedRuntime={} jobName={}",
                updatedRuntime, newJobName);
        updateStatusForNewJob(outputEvent, newJobName);
        futureTask.get();
        pool.shutdown();
        logger.info(getHandlerPhase(), "_mesage=\"job completed, future returned\" jobName={}", newJobName);
        jobRanSuccessfully = jobSucceeded(newJobName, outputEvent);
        if (jobRanSuccessfully)
          break;

      } catch (final Exception ex) {
        jobStarted = jobStartedSuccessfully(newJobName, outputEvent, ex);
        if (jobStarted) {
          break;
        }
      }
      if (attempts <= maxRetries) {
        logger.info(getHandlerPhase(), "will sleep for {} secs and retry", sleepBetweenRetriesMillis);
        try {
          Thread.sleep(sleepBetweenRetriesMillis);
        } catch (Exception ex) {
          logger.info(getHandlerPhase(), "Thread interrupted", ex);
        }
      }

    } while (!jobRanSuccessfully && attempts <= maxRetries);
  }

  private JobStatus updateStatusForNewJob(ActionEvent outputEvent, String jobName) throws RuntimeInfoStoreException {
    JobStatus newJobStatus = null;
    try {
      HiveJobSpec hiveJobSpec = new HiveJobSpec(jobName, inputDescriptor.getHiveConfDirectory());
      HiveJobStatus hiveJobStatus = hiveJobStatusFetcher.getStatusForJobWithRetry(hiveJobSpec);
      if (hiveJobStatus != null) {
        newJobStatus = hiveJobStatus.getOverallStatus();
        boolean updatedRuntime = recordRunningStatus(outputEvent, newJobStatus);
        logger.info(getHandlerPhase(),
                "_message=\"after submitting the job\" updatedRuntime={} jobID={} jobName={} runState={} runState={}",
                updatedRuntime, newJobStatus.getJobID().toString(), jobName, newJobStatus.getRunState(),
                JobStatus.getJobRunState(newJobStatus.getRunState()));
      }
    } catch (Exception ex) {
      logger.warn(getHandlerPhase(),
              "_message=\"processStatusForNewJob:unable to get the job status\" jobName={}", jobName, ex);
    }
    return newJobStatus;
  }

  private boolean jobSucceeded(String jobName, ActionEvent outputEvent)
          throws RuntimeInfoStoreException, JobStatusException {
    HiveJobSpec hiveJobSpec = new HiveJobSpec(jobName, inputDescriptor.getHiveConfDirectory());
    HiveJobStatus hiveJobStatus = hiveJobStatusFetcher.getStatusForJobWithRetry(hiveJobSpec);
    JobStatus completedJobStatus = null;
    boolean updatedRuntime = false;
    if (hiveJobStatus != null) {
      completedJobStatus = hiveJobStatus.getOverallStatus();
      JobStatus.State completeJobState = hiveJobStatus.getOverallStatus().getState();
      if (completeJobState == JobStatus.State.SUCCEEDED) {
        updatedRuntime = recordSuccessfulStatus(outputEvent, completedJobStatus);
      } else {
        updatedRuntime = recordFailedStatus(outputEvent, completedJobStatus);
      }
      logger.info(getHandlerPhase(),
              "_message=\"after job completion\" updatedRuntime={} jobID={} jobName={} runState={} runState={}",
              updatedRuntime, completedJobStatus.getJobID().toString(), jobName, completedJobStatus.getRunState(),
              JobStatus.getJobRunState(completedJobStatus.getRunState()));
      return completedJobStatus.getRunState() == JobStatus.SUCCEEDED;
    } else {
      logger.warn(getHandlerPhase(), "_message=\"after job completion, could not find job status\" jobName={} directory_path={}",
              jobName, hiveJobSpec.outputDirectoryPath());
      return false;
    }
  }

  private boolean recordRunningStatus(ActionEvent event, JobStatus jobStatus) throws RuntimeInfoStoreException {
    populateHeaders(event, jobStatus, ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_INTERIM_STATUS,
            JobStatus.getJobRunState(jobStatus.getRunState()));

    boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
            inputDescriptor.getInputDescriptorString(), io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.STARTED,
            event.getHeaders());

    return updatedRuntime;
  }

  private boolean recordSuccessfulStatus(ActionEvent event, JobStatus jobStatus) throws RuntimeInfoStoreException {
    populateHeaders(event, jobStatus, ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_COMPLETION_STATUS,
            JobStatus.getJobRunState(jobStatus.getRunState()));

    boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
            inputDescriptor.getInputDescriptorString(), io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.PENDING,
            event.getHeaders());

    return updatedRuntime;
  }

  private boolean recordFailedStatus(ActionEvent event, JobStatus jobStatus) throws RuntimeInfoStoreException {

    RuntimeInfo rtiRecord = getRuntimeInfoStore().get(AdaptorConfig.getInstance().getName(), getEntityName(),
            inputDescriptor.getInputDescriptorString());

    String origJobId = rtiRecord.getProperties()
            .get(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_ORIG_JOB_ID);

    if (StringHelper.isNotBlank(origJobId)) {
      origJobId = origJobId + "," + jobStatus.getJobID().toString();
    }
    populateHeaders(event, jobStatus, ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_JOB_COMPLETION_STATUS,
            JobStatus.getJobRunState(jobStatus.getRunState()));
    event.getHeaders().put(ActionEventHeaderConstants.HiveJDBCReaderHeaders.MAPRED_ORIG_JOB_ID, origJobId);

    boolean updatedRuntime = updateRuntimeInfo(getRuntimeInfoStore(), getEntityName(),
            inputDescriptor.getInputDescriptorString(), io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status.FAILED,
            event.getHeaders());

    return updatedRuntime;
  }

  protected boolean jobStartedSuccessfully(String jobName, ActionEvent outputEvent)
          throws RuntimeInfoStoreException, IOException, JobStatusException {
    return jobStartedSuccessfully(jobName, outputEvent, null);
  }

  protected boolean jobStartedSuccessfully(String jobName, ActionEvent outputEvent, Exception ex)
          throws RuntimeInfoStoreException, IOException, JobStatusException {
    HiveJobSpec hiveJobSpec = new HiveJobSpec(jobName, inputDescriptor.getHiveConfDirectory());
    HiveJobStatus hiveJobStatus = hiveJobStatusFetcher.getStatusForJobWithRetry(hiveJobSpec);
    if (hiveJobStatus != null) {
      JobStatus.State runState = hiveJobStatus.getOverallStatus().getState();
      String exMessage = "";
      if (ex != null)
        exMessage = ex.getMessage();

      // refactor below code to another method
      if (runState == JobStatus.State.RUNNING || runState == JobStatus.State.PREP) {
        boolean updatedRuntime = recordRunningStatus(outputEvent, hiveJobStatus.getOverallStatus());
        logger.warn(getHandlerPhase(),
                "_message=\"exception in running the job, but job was created successfully\" updatedRuntime={} jobName={} runState={} errorMessage={}",
                updatedRuntime, jobName, runState, exMessage, ex);
        return true;

      } else if (runState == JobStatus.State.SUCCEEDED) {
        boolean updatedRuntime = recordSuccessfulStatus(outputEvent, hiveJobStatus.getOverallStatus());
        logger.warn(getHandlerPhase(),
                "_message=\"exception in running the job, but job was completed successfully\" updatedRuntime={} jobName={} runState={}",
                updatedRuntime, jobName, runState, exMessage, ex);
        return true;
      } else {
        boolean updatedRuntime = recordFailedStatus(outputEvent, hiveJobStatus.getOverallStatus());
        logger.alert(Logger.ALERT_TYPE.INGESTION_FAILED, Logger.ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, Logger.ALERT_SEVERITY.NORMAL, ex, "_message=\"error in running the job\" updatedRuntime={} jobName={} runState={} error={}",
                updatedRuntime, jobName, runState, exMessage);
//                logger.warn(getHandlerPhase(),
//                        "_message=\"error in running the job\" updatedRuntime={} jobName={} runState={} error={}",
//                        updatedRuntime, jobName, runState, exMessage, ex);
        return false;
      }
    } else {
      return false;
    }

  }
}
