package io.bigdime.core.handler;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.AdaptorLogger;

public class AbstractSourceHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AbstractSourceHandler.class));

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
