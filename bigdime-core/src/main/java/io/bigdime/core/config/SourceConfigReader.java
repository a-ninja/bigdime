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
import io.bigdime.core.RequiredParameterMissingConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Map;

@Component
public final class SourceConfigReader {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(SourceConfigReader.class));

	@Autowired
	private JsonHelper jsonHelper;

	@Autowired
	private HandlerConfigReader handlerConfigReader;

	public SourceConfig readSourceConfig(final JsonNode sourceNode) throws AdaptorConfigurationException {
		logger.info("source build phase", "reading source config");

		final String name = jsonHelper.getRequiredStringProperty(sourceNode, SourceConfigConstants.NAME);
		final String description = jsonHelper.getRequiredStringProperty(sourceNode, SourceConfigConstants.DESCRIPTION);
		final String sourceType = jsonHelper.getRequiredStringProperty(sourceNode, SourceConfigConstants.SOURCE_TYPE);

		/*
		 * src-desc parameter under source defines the input parameters. One
		 * input can define multiple descriptors, say input1: "topic1:par1,
		 * par2" defines two descriptors, topic1:par1 and topica:par2.
		 *
		 */
		final JsonNode srcDesc = jsonHelper.getRequiredNode(sourceNode, SourceConfigConstants.SRC_DESC);
		if (srcDesc.size() < 1) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, "{} param must be specified", SourceConfigConstants.SRC_DESC);
			throw new RequiredParameterMissingConfigurationException(SourceConfigConstants.SRC_DESC);

		}
		Map<String, Object> srcDescMap = jsonHelper.getNodeTree(srcDesc);

		/*
		 * @formatter:off
		 * For each input, create a sourceConfig instance. There will be one Source built for each input, i.e.
		 * "src-desc": {
		 *   "input1" : "tab3,tab4",
		 *   "input2" : "topic1:par3,par1",
		 *   "input3" : "topic2:par1"
		 * },
		 * will have 5 Sources: one for input value: tab3, tab4, topic1:par3, topic1:par1, topic2:par1
		 * @formatter:on
		 */

		final SourceConfig sourceConfig = new SourceConfig();
		sourceConfig.setName(name);
		sourceConfig.setDescription(description);
		sourceConfig.setSrcDesc(srcDescMap);
		sourceConfig.setSourceType(sourceType);
		// Parse data-handlers
		final JsonNode handlerNodeArray = jsonHelper.getRequiredArrayNode(sourceNode,
				SourceConfigConstants.DATA_HANDLERS);
		initHandlerArray(sourceConfig);
		for (final JsonNode handlerNode : handlerNodeArray) {
			final HandlerConfig handlerConfig = handlerConfigReader.readHandlerConfig(handlerNode);
			sourceConfig.getHandlerConfigs().add(handlerConfig);
		}
		return sourceConfig;

	}

	private static void initHandlerArray(final SourceConfig sourceConfig) {
		// if (sourceConfig.getHandlerConfigs() == null) {
		final LinkedHashSet<HandlerConfig> handlers = new LinkedHashSet<>();
		sourceConfig.setHandlerConfigs(handlers);
		// }
	}
}
