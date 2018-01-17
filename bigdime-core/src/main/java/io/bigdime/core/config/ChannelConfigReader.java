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
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.config.AdaptorConfigConstants.ChannelConfigConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Reads the channel config from the json.
 *
 * @author Neeraj Jain
 *
 */
@Component
public final class ChannelConfigReader {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(ChannelConfigReader.class));
	@Autowired
	private JsonHelper jsonHelper;

	/**
	 * Constructs a ChannelConfig from the given JsonNode object. ` * @param
	 * channelNode JsonNode encapsulating the configuration values needed to
	 * build a {@link ChannelConfig} object
	 *
	 * @return new ChannelConfig object with information needed to build a
	 *         {@link io.bigdime.core.DataChannel} object.
	 * @throws AdaptorConfigurationException
	 *             if there was a problem in reading the configuration.
	 * 
	 *             TODO: Throw
	 *             {@link io.bigdime.core.RequiredParameterMissingConfigurationException}
	 *             wherever applicable
	 */
	public ChannelConfig readChannelConfig(final JsonNode channelNode) throws AdaptorConfigurationException {
		logger.info("channel build phase", "reading channel config");
		try {
			final ChannelConfig channelConfig = new ChannelConfig();
			channelConfig.setName(jsonHelper.getRequiredStringProperty(channelNode, ChannelConfigConstants.NAME));
			channelConfig.setDescription(
					jsonHelper.getRequiredStringProperty(channelNode, ChannelConfigConstants.DESCRIPTION));
			channelConfig.setChannelClass(
					jsonHelper.getRequiredStringProperty(channelNode, ChannelConfigConstants.CHANNEL_CLASS));

			final JsonNode properties = jsonHelper.getRequiredNode(channelNode, ChannelConfigConstants.PROPERTIES);
			final Map<String, Object> channelProperties = jsonHelper.getNodeTree(properties);

			channelConfig.setChannelProperties(channelProperties);
			return channelConfig;
		} catch (Exception e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, "channel could not be configured");
			throw new AdaptorConfigurationException(e);
		}
	}

}
