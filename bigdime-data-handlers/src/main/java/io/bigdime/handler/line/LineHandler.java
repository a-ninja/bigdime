/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.line;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.Segment;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.handler.AbstractSourceHandler;

@Component
@Scope("prototype")
public class LineHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(LineHandler.class));

	private String handlerPhase;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building LineHandler";

	}

	@Override
	public Status process() throws HandlerException {

		handlerPhase = "processing LineHandler";

		logger.debug(handlerPhase, "processing LineHandler");
		LineHandlerJournal journal = getJournal(LineHandlerJournal.class);
		List<ActionEvent> eventList = null;
		if (journal != null) {
			if (journal.getEventList() != null && !journal.getEventList().isEmpty()) {
				/*
				 * @formatter:off
				 * If there are events in journal 
				 * 	process partialLineData and eventsList from journal 
				 * @formatter:on
				 */
				ActionEvent leftoverEvent = journal.getLeftoverEvent();
				eventList = journal.getEventList();
				logger.debug(handlerPhase, "_message=\"journal not empty\" journal_list_size={}", eventList.size());
				if (leftoverEvent != null) {
					logger.debug(handlerPhase, "_message=\"found an event with leftover data\"");
					eventList.add(0, leftoverEvent);
				}

			} else {
				/*
				 * @formatter:off
				 * else if there are no events in journal
				 * 	process partialLineData from journal and eventList from context.
				 * @formatter:on
				 */
				ActionEvent leftoverEvent = journal.getLeftoverEvent();
				eventList = new ArrayList<>(getHandlerContext().getEventList());
				logger.debug(handlerPhase, "_message=\"journal has left over data\" context_list_size={}",
						eventList.size());
				eventList.add(0, leftoverEvent);
			}

		} else {
			/*
			 * @formatter:off
			 * else if journal is null
			 * 	process eventList from context
			 * @formatter:on
			 */
			eventList = new ArrayList<>(getHandlerContext().getEventList());
			logger.debug(handlerPhase, "_message=\"journal is null\" context_list_size={}", eventList.size());
		}
		Preconditions.checkNotNull(eventList, "eventList must be not null");
		Preconditions.checkArgument(!eventList.isEmpty(), "eventList in must contain at least one ActionEvent");

		return processIt(eventList);

	}

	/*
	 * @formatter:off
	 * 
	 * use cases:
	 * event list has 1 event
	 * 	if it has new line char and data after that.
	 * 		an event with new line goes to handler context
	 * 		rest data goes to journal
	 * 		return ready
	 * 	else if it has new line char and NO data after that.
	 * 		an event with new line goes to handler context
	 * 		set journal to null
	 * 		return ready
	 * 	else if it has NO new line char
	 * 		all data goes to journal
	 * 		return ready
	 * 
	 * 
	 * event list has 10 events
	 * 	if an event has new line and data after that
	 * 		an event with new line goes to handler context
	 * 		rest data go to journal as partialLineData
	 * 		other events go to journal, as eventList
	 * 		submit to channel, if applicable
	 * 		return callback
	 * 	else if an event has new line and no data after that
	 * 		an event with new line goes to handler context
	 * 		other events go to journal, as eventList
	 * 		submit to channel, if applicable
	 * 		return callback
	 * 	else if no event has new line
	 * 		all events go to journal, as partialLineData
	 * 		return ready
	 * 
	 * 
	 * Flow:
	 * If there are events in journal
	 * 	process partialLineData and eventsList from journal
	 * else if there is no event in journal
	 * 	process partialLineData and eventList from context.
	 * 
	 * 
	 * 
	 * @formatter:on
	 */
	private Status processIt(List<ActionEvent> mutableList) throws HandlerException {

		Status statusToReturn = Status.READY;

		Iterator<ActionEvent> actionEventIter = mutableList.iterator();

		byte[] lineData = null;
		ActionEvent leftoverEvent = null;
		ActionEvent outputEvent = null;
		Map<String, String> headers = null;
		getHandlerContext().setEventList(null);
		/*
		 * For each event if the event contains a line process it else get next
		 * event
		 */
		while (actionEventIter.hasNext()) {
			ActionEvent actionEvent = actionEventIter.next();
			actionEventIter.remove();
			headers = actionEvent.getHeaders();

			if (lineData == null)
				lineData = actionEvent.getBody();
			else
				lineData = ArrayUtils.addAll(lineData, actionEvent.getBody());

			Segment lineDataSegment = StringHelper.partitionByNewLine(lineData);
			/*
			 * If the newline was found in dataToPartition, create an event for
			 * this and set in HandlerContext. Put rest of the data in another
			 * event in journal.
			 * 
			 */
			if (lineDataSegment != null) {
				outputEvent = new ActionEvent();
				outputEvent.setBody(lineDataSegment.getLines());
				logger.debug(handlerPhase, "headers={}", actionEvent.getHeaders());
				outputEvent.setHeaders(actionEvent.getHeaders());
				getHandlerContext().createSingleItemEventList(outputEvent);
				if (lineDataSegment.getLeftoverData() != null && lineDataSegment.getLeftoverData().length != 0) {
					leftoverEvent = new ActionEvent();
					leftoverEvent.setBody(lineDataSegment.getLeftoverData());
					leftoverEvent.setHeaders(actionEvent.getHeaders());
					// mutableList.add(leftoverEvent);
				}
				lineData = null;
				break;
			}
		}

		LineHandlerJournal journal = getJournal(LineHandlerJournal.class);
		if (journal == null) {
			journal = new LineHandlerJournal();
			getHandlerContext().setJournal(getId(), journal);
		}
		/*
		 * If there was an output event
		 */
		if (outputEvent != null)
			processChannelSubmission(outputEvent);

		if (leftoverEvent != null) {
			journal.setLeftoverEvent(leftoverEvent);
			statusToReturn = Status.READY;
		} else {
			leftoverEvent = new ActionEvent();
			leftoverEvent.setBody(lineData);
			leftoverEvent.setHeaders(headers);
			journal.setLeftoverEvent(leftoverEvent);
			statusToReturn = Status.READY;
		}
		if (!mutableList.isEmpty()) {
			journal.setEventList(mutableList);
			statusToReturn = Status.CALLBACK;
		}

		return statusToReturn;
	}

}
