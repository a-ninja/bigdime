/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
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
import io.bigdime.core.DataChannel;
import io.bigdime.core.InvalidDataTypeConfigurationException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.RequiredParameterMissingConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants.ChannelConfigConstants;
import io.bigdime.core.config.ChannelConfig;

/**
 * Builds channels based on the {@link ChannelConfig} object.
 * 
 * @author Neeraj Jain
 *
 */
@Component
public final class ChannelFactory {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(ChannelFactory.class));

	@Autowired
	private ApplicationContext context;
	private Properties appProperties;

	public ChannelFactory() throws AdaptorConfigurationException {
		String envProperties = System.getProperty("env.properties");
		if (envProperties == null) {
			envProperties = "application.properties";
			logger.info("constructing ChannelFactory with default envProperties", "envProperties=\"{}\"",
					envProperties);
		}
		logger.info("constructing ChannelFactory", "envProperties=\"{}\"", envProperties);
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(envProperties)) {
			appProperties = new Properties();
			appProperties.load(is);
			logger.info("constructing ChannelFactory", "properties=\"{}\"", appProperties.toString());
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}

	/**
	 * Get the collection of {@link DataChannel} for given {@link ChannelConfig}
	 * object. One {@link ChannelConfig} object can result into more than one
	 * {@link DataChannel} hence this method returns a collection.
	 * 
	 * @param channelConfig
	 *            {@link ChannelConfig} object parsed from the adaptor
	 *            configuration file.
	 * @return Collection of DataChannel objects
	 * @throws AdaptorConfigurationException
	 * @throws NullPointerException
	 *             if the ChannelConfig doesn't contain channelClass.
	 */
	public Collection<DataChannel> getChannel(final ChannelConfig channelConfig) throws AdaptorConfigurationException {
		try {
			Collection<DataChannel> channels = new HashSet<>();
			if (channelConfig == null) {
				throw new NullPointerException(
						"channelConfig must be not null and must return a valid value for getChannelClass");
			}
			if (channelConfig.getChannelClass() == null) {
				throw new RequiredParameterMissingConfigurationException(
						"channelConfig must return a valid value for getChannelClass");
			}
			Object concurrency = channelConfig.getChannelProperties().get(ChannelConfigConstants.CONCURRENCY);
			int channelInstances = 1;
			try {
				if (concurrency != null) {
					channelInstances = Integer.valueOf(
							channelConfig.getChannelProperties().get(ChannelConfigConstants.CONCURRENCY).toString());
					if (channelInstances < 1) {
						logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
								ALERT_SEVERITY.BLOCKER, "{} param must be a positive number",
								ChannelConfigConstants.CONCURRENCY);
						throw new InvalidValueConfigurationException(
								ChannelConfigConstants.CONCURRENCY + " must be a positive number");
					}
				}
				for (int i = 0; i < channelInstances; i++) {
					logger.debug("building channel", "channel_name=\"{}\" instance=\"{} of {}\"",
							channelConfig.getName(), i + 1, channelInstances);
					final Class<? extends DataChannel> channelClass = Class.forName(channelConfig.getChannelClass())
							.asSubclass(DataChannel.class);
					final DataChannel channel = context.getBean(channelClass);

					Map<String, Object> channelProperties = channelConfig.getChannelProperties();
					PropertyHelper.redeemTokensFromAppProperties(channelProperties, appProperties);

					channel.setPropertyMap(channelConfig.getChannelProperties());
					channel.setName(channelConfig.getName());
					channel.build();
					channels.add(channel);
				}
			} catch (NumberFormatException e) {
				logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
						ALERT_SEVERITY.BLOCKER, "{} param must be of number type", ChannelConfigConstants.CONCURRENCY);
				throw new InvalidDataTypeConfigurationException(ChannelConfigConstants.CONCURRENCY, "number");

			}
			return channels;
		} catch (ClassNotFoundException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}
}
