package io.bigdime.handler.hive;

import java.io.File;
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

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.handler.hive.HiveJdbcReaderHandlerConstants.AUTH_OPTION;

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
public class HiveJdbcReaderHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HiveJdbcReaderHandler.class));
	private String handlerPhase = "building HiveReaderHandler";

	private String outputDirectory = "";

	private String jdbcUrl = null;// e.g.
									// jdbc:hive2://host:10000/default;principal=hadoop/host@DOMAIN.COM
	private String driverClassName = null;
	private AUTH_OPTION authOption;

	private String kerberosUserName = null;// e.g. user@domain.com
	private String kerberosKeytabPath = null; // e.g. /tmp/user.keytab

	private String userName = null;// e.g. username
	private String password = null; // e.g. password

	private String baseOutputDirectory = null;

	private String entityName = null;
	private String hiveQuery = null;
	private final Map<String, String> hiveConfigurations = new HashMap<>();

	@Autowired
	HiveJdbcConnectionFactory hiveJdbcConnectionFactory;

	private Connection connection = null;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building HiveJdbcReaderHandler";
		logger.info(handlerPhase, "properties={}", getPropertyMap());

		Map<String, Object> properties = getPropertyMap();
		for (String key : properties.keySet()) {
			logger.debug(handlerPhase, "key=\"{}\" value=\"{}\"", key, getPropertyMap().get(key));
		}

		// sanity check for src-desc
		@SuppressWarnings("unchecked")
		Entry<Object, String> srcDescEntry = (Entry<Object, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescEntry == null) {
			throw new InvalidValueConfigurationException("src-desc can't be null");
		}

		logger.debug(handlerPhase, "src-desc-node-key=\"{}\" src-desc-node-value=\"{}\"", srcDescEntry.getKey(),
				srcDescEntry.getValue());

		@SuppressWarnings("unchecked")
		Map<String, Object> srcDescValueMap = (Map) srcDescEntry.getKey();

		entityName = PropertyHelper.getStringProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.ENTITY_NAME);
		hiveQuery = PropertyHelper.getStringProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_QUERY);
		logger.debug(handlerPhase, "entityName=\"{}\" hiveQuery=\"{}\"", entityName, hiveQuery);

		for (String key : srcDescValueMap.keySet()) {
			logger.debug(handlerPhase, "srcDesc-key=\"{}\" srcDesc-value=\"{}\"", key, srcDescValueMap.get(key));
		}

		setHiveConfigurations(srcDescValueMap);

		// Set JDBC params
		jdbcUrl = PropertyHelper.getStringProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.JDBC_URL);
		driverClassName = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.DRIVER_CLASS_NAME);
		final String authChoice = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.AUTH_CHOICE, AUTH_OPTION.KERBEROS.toString());

		authOption = AUTH_OPTION.getByName(authChoice);

		kerberosUserName = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.KERBEROS_USER_NAME);
		kerberosKeytabPath = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.KERBEROS_KEYTAB_PATH);

		userName = PropertyHelper.getStringProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.USER_NAME);
		password = PropertyHelper.getStringProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.PASSWORD);

		logger.debug(handlerPhase,
				"jdbcUrl=\"{}\" driverClassName=\"{}\" authChoice={} authOption={} kerberosUserName=\"{}\" kerberosKeytabPath=\"{}\" userName=\"{}\" password=\"{}\"",
				jdbcUrl, driverClassName, authChoice, authOption, kerberosUserName, kerberosKeytabPath, userName,
				password);

		baseOutputDirectory = PropertyHelper.getStringProperty(getPropertyMap(),
				HiveJdbcReaderHandlerConstants.BASE_OUTPUT_DIRECTORY, "/");

	}

	@SuppressWarnings("unchecked")
	private void setHiveConfigurations(Map<String, Object> srcDescValueMap) {
		if (getPropertyMap().get(HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
			logger.debug(handlerPhase, "found hive-conf in handler properties");
			hiveConfigurations
					.putAll(PropertyHelper.getMapProperty(getPropertyMap(), HiveJdbcReaderHandlerConstants.HIVE_CONF));
			logger.debug(handlerPhase, "hiveConfs from handler properties=\"{}\"", hiveConfigurations);
		}

		if (PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF) != null) {
			logger.debug(handlerPhase, "found hive-conf in src-desc properties");
			hiveConfigurations
					.putAll(PropertyHelper.getMapProperty(srcDescValueMap, HiveJdbcReaderHandlerConstants.HIVE_CONF));
		}
		logger.debug(handlerPhase, "hiveConfs=\"{}\"", hiveConfigurations);

	}

	private void setHdfsOutputDirectory() {
		final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd");
		if (!baseOutputDirectory.endsWith(File.separator))
			outputDirectory = baseOutputDirectory + File.separator;
		else
			outputDirectory = baseOutputDirectory;
		String dateTime = dtf.print(System.currentTimeMillis());
		outputDirectory = outputDirectory + dateTime + File.separator + entityName;
		logger.debug(handlerPhase, "outputDirectory=\"{}\"", outputDirectory);

		// hiveConfigurations.put(dataset, dataset)
		hiveConfigurations.put("DIRECTORY", outputDirectory);
		hiveConfigurations.put("DATE", dateTime);
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing HiveJdbcReaderHandler";
		incrementInvocationCount();
		logger.debug(handlerPhase, "_message=\"entering process\"");
		try {
			setHdfsOutputDirectory();
			setupConnection();
			ActionEvent outputEvent = new ActionEvent();
			final Statement stmt = connection.createStatement();
			runHiveConfs(stmt);

			logger.debug(handlerPhase, "hiveQuery=\"{}\" hiveConfigurations=\"{}\"", hiveQuery, hiveConfigurations);
			// stmt.execute(hiveQuery);// no resultset is returned
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH, outputDirectory);
			getHandlerContext().createSingleItemEventList(outputEvent);
			logger.debug(handlerPhase, "_message=\"completed process\"");
		} catch (final Exception e) {
			throw new HandlerException("unable to process", e);
		} finally {
			closeConnection();
		}

		return Status.READY;
	}

	private void runHiveConfs(final Statement stmt) throws SQLException {
		if (hiveConfigurations != null) {
			for (final String prop : hiveConfigurations.keySet()) {
				String sql = "set " + prop + " = " + hiveConfigurations.get(prop);
				logger.debug(handlerPhase, "running sql to set hiveconf: \"{}\"", sql);
				stmt.execute(sql);
			}
		}
	}

	private void setupConnection() throws SQLException, IOException, ClassNotFoundException {
		if (connection == null) {
			if (authOption == AUTH_OPTION.KERBEROS) {
				connection = hiveJdbcConnectionFactory.getConnectionWithKerberosAuthentication(driverClassName, jdbcUrl,
						kerberosUserName, kerberosKeytabPath, hiveConfigurations);
				logger.debug(handlerPhase, "_message=\"connected to db\"");
			} else if (authOption == AUTH_OPTION.PASSWORD) {
				connection = hiveJdbcConnectionFactory.getConnection(driverClassName, jdbcUrl, userName, password,
						hiveConfigurations);
				logger.debug(handlerPhase, "_message=\"connected to db\"");
			}
		}
	}

	private void closeConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (Exception e) {
			logger.warn(handlerPhase, "_message=\"error while trying to close the connection\"", e);
		} finally {
			connection = null;
		}
	}

}
