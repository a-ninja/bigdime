package io.bigdime.handler.swift;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.constants.ActionEventHeaderConstants;

/**
 * 
 * @author Neeraj Jain
 *
 */

@Component
@Scope("prototype")
public final class SwiftByteWriterHandler extends AbstractByteWriterHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(SwiftByteWriterHandler.class));

	@Override
	public Status process() throws HandlerException {
		setHandlerPhase("processing SwiftByteWriterHandler");
		return super.process();
	}

	@Override
	protected Status process0(List<ActionEvent> actionEvents) throws HandlerException {

		SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
		if (journal == null) {
			logger.debug(getHandlerPhase(), "jounral is null, initializing");
			journal = new SwiftWriterHandlerJournal();
			getHandlerContext().setJournal(getId(), journal);
		}
		Status statusToReturn = Status.READY;
		long startTime = System.currentTimeMillis();
		try {

			boolean writeReady = false;
			int numEventsToWrite = 0;
			ActionEvent actionEventToWrite = null;
			for (ActionEvent actionEvent : actionEvents) {
				numEventsToWrite++;
				String readCompleteFlag = actionEvent.getHeaders().get(ActionEventHeaderConstants.READ_COMPLETE);

				if (!StringUtils.isBlank(readCompleteFlag) && Boolean.parseBoolean(readCompleteFlag) == true) {
					writeReady = true;
					actionEventToWrite = actionEvent;
					logger.debug(getHandlerPhase(), "_message=\"found readComplete\" headers={} eventsToWrite={}",
							actionEvent.getHeaders(), numEventsToWrite);
					break;
				}
			}

			// If writeReady, write to swift and clear those events from list.
			if (writeReady) {
				logger.debug(getHandlerPhase(), "_message=\"calling writeToSwift\" numEventsToWrite={}",
						numEventsToWrite);
				List<ActionEvent> eventListToWrite = actionEvents.subList(0, numEventsToWrite);
				ActionEvent outputEvent = writeToSwift(actionEventToWrite, eventListToWrite);

				getHandlerContext().createSingleItemEventList(outputEvent);
				journal.reset();
				eventListToWrite.clear();// clear the processed events from the
											// list
			}

			if (!actionEvents.isEmpty()) {
				journal.setEventList(actionEvents);
			}

			// if we wrote and there is more data to write, callback
			if (writeReady) {
				if (!actionEvents.isEmpty()) {
					statusToReturn = Status.CALLBACK;
				} else {
					statusToReturn = Status.READY;
				}
			} else {
				statusToReturn = Status.BACKOFF; // no need to call next handler
			}
		} catch (Exception e) {
			throw new HandlerException(e.getMessage(), e);
		}
		long endTime = System.currentTimeMillis();
		logger.debug(getHandlerPhase(), "statusToReturn={}", statusToReturn);
		logger.info(getHandlerPhase(), "SwiftByteWriterHandler finished in {} milliseconds", (endTime - startTime));
		return statusToReturn;

	}

}
