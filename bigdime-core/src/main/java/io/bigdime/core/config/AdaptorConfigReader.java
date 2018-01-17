/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

//import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * AdaptorConfigReader implements methods to validate and read the adaptor
 * configuration file.
 *
 * @author Neeraj Jain
 *
 */
@Component
public class AdaptorConfigReader {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AdaptorConfigReader.class));

	@Autowired
	private SourceConfigReader sourceConfigReader;

	@Autowired
	private ChannelConfigReader channelConfigReader;

	@Autowired
	private SinkConfigReader sinkConfigReader;

	@Autowired
	private JsonHelper jsonHelper;
	/*
	 * config parameters for adaptor node.
	 */
	private static final String NAME = "name";
	private static final String TYPE = "type";
	private static final String NAMESPACE = "namespace";
	private static final String DESCRIPTION = "description";
	private static final String SOURCE_NODE = "source";
	private static final String SINK_NODE = "sink";
	private static final String CHANNEL_NODE = "channel";
	private static final String CRON_EXPRESSION = "cron-expression";
	private static final String AUTO_START = "auto-start";
	private static String CONFIG_FILE_LOCATION = "META-INF/adaptor.json";

	// public boolean isConfigValid(final String configFile) {
	// return true;
	// }

	/**
	 * Read the adaptor configuration file from META-INF/adaptor.json file.
	 *
	 * @return AdaptorConfig object that represents the adaptor configuration
	 * @throws AdaptorConfigurationException
	 *             if there was any problem in reading or parsing the
	 *             configuration file
	 */
	public void readConfig(final AdaptorConfig config) throws AdaptorConfigurationException {
		/*
		 * read json read name, description, namespace and set in adaptor config
		 * read source and invoke source builder with json node.
		 */

		final JsonNode adaptorNode = parseJsonFile();
		// final AdaptorConfig config = new AdaptorConfig();

		config.setName(jsonHelper.getRequiredStringProperty(adaptorNode, NAME));
		config.setType(ADAPTOR_TYPE.getByValue(jsonHelper.getRequiredStringProperty(adaptorNode, TYPE)));
		config.setDescription(jsonHelper.getRequiredStringProperty(adaptorNode, DESCRIPTION));
		config.setNamespace(jsonHelper.getRequiredStringProperty(adaptorNode, NAMESPACE));
		if (config.getType() == ADAPTOR_TYPE.BATCH)
			config.setCronExpression(jsonHelper.getRequiredStringProperty(adaptorNode, CRON_EXPRESSION));

		config.setAutoStart(jsonHelper.getBooleanProperty(adaptorNode, AUTO_START));

		logger.info("reading config", "name=\"{}\" description=\"{}\" namespace=\"{}\"", config.getName(),
				config.getDescription(), config.getNamespace());

		final SourceConfig sourceConfig = readSourceConfig(adaptorNode);

		final Set<SinkConfig> sinkConfigs = readSinkConfig(adaptorNode);

		final Set<ChannelConfig> channelConfigs = readChannelConfig(adaptorNode);

		config.setSourceConfig(sourceConfig);
		config.setChannelConfigs(channelConfigs);
		config.setSinkConfigs(sinkConfigs);
	}

	private JsonNode parseJsonFile() throws AdaptorConfigurationException {
		logger.info("reading config", "CONFIG_FILE_LOCATION=\"{}\"", CONFIG_FILE_LOCATION);
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(CONFIG_FILE_LOCATION)) {
			return jsonHelper.readStream(is);
		}
//		catch (JsonProcessingException e) {
//			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
//					ALERT_SEVERITY.BLOCKER, "adaptor configuration file could not be parsed");
//			throw new AdaptorConfigurationException(e);
//		}
		catch (IOException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, "adaptor configuration file could not be found or could not be read");
			throw new AdaptorConfigurationException(e);
		}
	}

	private SourceConfig readSourceConfig(JsonNode adaptorNode) throws AdaptorConfigurationException {
		final JsonNode sourceNode = jsonHelper.getRequiredNode(adaptorNode, SOURCE_NODE);
		if (!sourceNode.iterator().hasNext()) {
			throw new AdaptorConfigurationException("empty source block is not allowed adaptor configuration file");
		}
		final SourceConfig sourceConfig = sourceConfigReader.readSourceConfig(sourceNode);
		return sourceConfig;
	}

	private Set<ChannelConfig> readChannelConfig(JsonNode adaptorNode) throws AdaptorConfigurationException {
		final JsonNode channelNodeArray = jsonHelper.getRequiredArrayNode(adaptorNode, CHANNEL_NODE);
		final Set<ChannelConfig> channelConfigs = new HashSet<>();
		for (JsonNode channelNode : channelNodeArray) {
			channelConfigs.add(channelConfigReader.readChannelConfig(channelNode));
		}
		return channelConfigs;
	}

	private Set<SinkConfig> readSinkConfig(JsonNode adaptorNode) throws AdaptorConfigurationException {
		final JsonNode sinkNodeArray = jsonHelper.getRequiredArrayNode(adaptorNode, SINK_NODE);
		if (sinkNodeArray == null || sinkNodeArray.size() < 1) {
			throw new AdaptorConfigurationException("no sink found in adaptor configuration file");
		}
		final Set<SinkConfig> sinkConfigs = new HashSet<>();
		for (JsonNode sinkNode : sinkNodeArray) {
			if (!sinkNode.iterator().hasNext()) {
				throw new AdaptorConfigurationException("empty sink block is not allowed adaptor configuration file");
			}
			sinkConfigs.add(sinkConfigReader.readSinkConfig(sinkNode));
		}
		return sinkConfigs;
	}

}
