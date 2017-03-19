/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import com.fasterxml.jackson.databind.JsonNode;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.config.AdaptorConfigConstants.HandlerConfigConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public final class HandlerConfigReader {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HandlerConfigReader.class));

	@Autowired
	private JsonHelper jsonHelper;

	public HandlerConfig readHandlerConfig(final JsonNode handlerNode) throws AdaptorConfigurationException {
		logger.debug("reading handler config", "reading handler config");
		final HandlerConfig handlerConfig = new HandlerConfig();

		final String name = jsonHelper.getRequiredStringProperty(handlerNode, HandlerConfigConstants.NAME);
		final String description = jsonHelper.getRequiredStringProperty(handlerNode,
				HandlerConfigConstants.DESCRIPTION);
		final String handlerClass = jsonHelper.getRequiredStringProperty(handlerNode,
				HandlerConfigConstants.HANDLER_CLASS);
		handlerConfig.setName(name);
		handlerConfig.setDescription(description);
		final Map<String, Object> handlerProperties = new HashMap<>();
		handlerConfig.setHandlerProperties(handlerProperties);// set a not null

		final JsonNode properties = jsonHelper.getOptionalNodeOrNull(handlerNode, HandlerConfigConstants.PROPERTIES);
		if (properties != null)
			handlerProperties.putAll(jsonHelper.getNodeTree(properties));

		logger.debug("reading handler config", "handlerProperties=\"{}\"", handlerProperties);

		handlerConfig.setHandlerClass(handlerClass);

		return handlerConfig;
	}

}
