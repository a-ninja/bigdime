/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import com.fasterxml.jackson.databind.JsonNode;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.InvalidDataTypeConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.config.AdaptorConfigConstants.SinkConfigConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;

@Component
public final class SinkConfigReader {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(SinkConfigReader.class));

	@Autowired
	private JsonHelper jsonHelper;

	@Autowired
	private HandlerConfigReader handlerConfigReader;

	public SinkConfig readSinkConfig(final JsonNode sinkNode) throws AdaptorConfigurationException {
		logger.info("sink build phase", "reading sink config");

		final SinkConfig sinkConfig = new SinkConfig();
		try {

			final String name = jsonHelper.getRequiredStringProperty(sinkNode, SinkConfigConstants.NAME);
			final String description = jsonHelper.getRequiredStringProperty(sinkNode, SinkConfigConstants.DESCRIPTION);
			final JsonNode channelDescArray = jsonHelper.getRequiredArrayNode(sinkNode,
					SinkConfigConstants.CHANNEL_DESC);
			logger.info("sink build phase", "channelDescArray=\"{}\"", channelDescArray);
			final Collection<String> channelDescs = new HashSet<>();
			for (JsonNode channelDesc : channelDescArray) {
				if (channelDesc.isTextual()) {
					channelDescs.add(channelDesc.asText());
				} else {
					logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
							ALERT_SEVERITY.BLOCKER, "{} param must be specified", SinkConfigConstants.CHANNEL_DESC);
					throw new InvalidDataTypeConfigurationException(SinkConfigConstants.CHANNEL_DESC, "text node");
				}
			}

			sinkConfig.setName(name);
			sinkConfig.setDescription(description);
			sinkConfig.setChannelDescs(channelDescs);

			final JsonNode handlerNodeArray = jsonHelper.getRequiredArrayNode(sinkNode,
					SinkConfigConstants.DATA_HANDLERS);

			initHandlerArray(sinkConfig);
			for (final JsonNode handlerNode : handlerNodeArray) {
				final HandlerConfig handlerConfig = handlerConfigReader.readHandlerConfig(handlerNode);
				/*
				 * Set a unique name for the handler by prefixing it with sink
				 * name. DataChannel needs to know unique consumers.
				 */
				handlerConfig.setName(name + "-" + handlerConfig.getName());
				sinkConfig.getHandlerConfigs().add(handlerConfig);
			}
		} catch (IllegalArgumentException ex) {
			throw new AdaptorConfigurationException(ex);
		}
		return sinkConfig;
	}

	/**
	 * Initialize handlers for sinkConfig if it's null.
	 *
	 * @param sinkConfig
	 */
	private static void initHandlerArray(final SinkConfig sinkConfig) {
		// if (sinkConfig.getHandlerConfigs() == null) {
		final LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		sinkConfig.setHandlerConfigs(handlerConfigs);
		// }
	}
}