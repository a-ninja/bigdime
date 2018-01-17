/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.HandlerConfig;

@Component
public final class HandlerFactory {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HandlerFactory.class));
	@Autowired
	private ApplicationContext context;
	private Properties appProperties;

	public HandlerFactory() throws AdaptorConfigurationException {
		String envProperties = System.getProperty("env.properties");
		if (envProperties == null) {
			envProperties = "application.properties";
			logger.info("constructing HandlerFactory with default envProperties", "envProperties=\"{}\"",
					envProperties);
		}
		logger.info("constructing HandlerFactory", "envProperties=\"{}\"", envProperties);
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(envProperties)) {
			appProperties = new Properties();
			appProperties.load(is);
			logger.info("constructing HandlerFactory", "properties=\"{}\"", appProperties.toString());
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}

	public Handler getHandler(final HandlerConfig handlerConfig) throws AdaptorConfigurationException {
		try {
			final Class<? extends Handler> handlerClass = Class.forName(handlerConfig.getHandlerClass())
					.asSubclass(Handler.class);
			Handler handler = context.getBean(handlerClass);
			handler.setHandlerClass(handlerClass);

			Map<String, Object> handlerProperties = handlerConfig.getHandlerProperties();
			PropertyHelper.redeemTokensFromAppProperties(handlerProperties, appProperties);
			handler.setPropertyMap(Collections.unmodifiableMap(handlerConfig.getHandlerProperties()));
			handler.setName(handlerConfig.getName());
			handler.build();
			logger.debug("building handler", "handler_name=\"{}\" handler_properties=\"{}\"", handler.getName(),
					handlerConfig.getHandlerProperties());
			return handler;
		} catch (ClassNotFoundException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}
}
