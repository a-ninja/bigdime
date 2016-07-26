/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.SinkConfigConstants;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

/**
 * Handler that reads data from memory channel.
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class MemoryChannelInputHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(MemoryChannelInputHandler.class));

	private DataChannel inputChannel;

	private int batchSize;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		setHandlerPhase("building MemoryChannelInputHandler");
		logger.info(getHandlerPhase(), "building MemoryChannelInputHandler");
		final String channelDesc = (String) getPropertyMap().get(SinkConfigConstants.CHANNEL_DESC);
		final Map<String, DataChannel> channelMap = AdaptorConfig.getInstance().getAdaptorContext().getChannelMap();
		inputChannel = channelMap.get(channelDesc);
		batchSize = PropertyHelper.getIntProperty(getPropertyMap(), MemoryChannelInputHandlerConstants.BATCH_SIZE,
				MemoryChannelInputHandlerConstants.DEFAULT_BATCH_SIZE);
		logger.info(getHandlerPhase(), "handler_name=\"{}\" channelDesc=\"{}\" inputChannel=\"{}\" batchSize=\"{}\"",
				getName(), channelDesc, inputChannel, batchSize);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Status doProcess() throws IOException, RuntimeInfoStoreException, HandlerException {
		try {
			logger.debug(getHandlerPhase(),
					"consumer_name=\"{}\" channel_name=\"{}\" inputChannel=\"{}\" current_thread=\"{}\"", getName(),
					inputChannel.getName(), inputChannel, Thread.currentThread().getId());
			@SuppressWarnings("rawtypes")
			List took = inputChannel.take(getName(), batchSize);
			logger.debug(getHandlerPhase(), "consumer_name=\"{}\" channel_name=\"{}\" took_event.size=\"{}\"",
					getName(), inputChannel.getName(), took.size());
			getHandlerContext().setEventList((List<ActionEvent>) took);
			return Status.READY;
		} catch (ChannelException e) {
			logger.debug(getHandlerPhase(),
					"_message=\"MemoryChannelInputHandler didn't receive data\" consumer_name=\"{}\" channel_name=\"{}\" reason=\"{}\"",
					getName(), inputChannel.getName(), e.getMessage());
			return Status.BACKOFF;
		}
	}
}
