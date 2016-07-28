package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.AdaptorLogger;

public class AbstractSourceHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AbstractSourceHandler.class));

	public List<String> getAvailableDirectoriesFromHeader(final String headerName) {
		logger.info(getHandlerPhase(), "_message=\"getAvailableDirectoriesFromHeader\" handler_id={} headerName=\"{}\"",
				getId(), headerName);
		List<ActionEvent> eventList = getHandlerContext().getEventList();
		List<String> availableHdfsDirectories = new ArrayList<>();
		for (final ActionEvent inputEvent : eventList) {
			logger.info(getHandlerPhase(),
					"_message=\"getAvailableDirectoriesFromHeader\" handler_id={} headerName=\"{}\" header_value={}",
					getId(), headerName, inputEvent.getHeaders().get(headerName));
			availableHdfsDirectories.add(inputEvent.getHeaders().get(headerName));
		}
		return availableHdfsDirectories;
	}

	/**
	 * @param outputEvent
	 */
	protected void processChannelSubmission(final ActionEvent outputEvent) {
		logger.debug(getHandlerPhase(), "checking channel submission output_channel=\"{}\"", getOutputChannel());

		if (getOutputChannel() != null) {
			if (outputEvent != null) {
				logger.debug(getHandlerPhase(), "submitting to channel, headers={} output_channel=\"{}\"",
						outputEvent.getHeaders(), getOutputChannel().getName());
			}
			getOutputChannel().put(outputEvent);
		}
	}

}
