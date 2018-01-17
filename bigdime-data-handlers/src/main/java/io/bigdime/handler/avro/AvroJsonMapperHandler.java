/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.libs.avro.AvroMessageEncoderDecoder;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * A handler that receives the avro document, converts to json and, optionally,
 * submits to channel.
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class AvroJsonMapperHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AvroJsonMapperHandler.class));

	private String schemaFileName;
	private Integer bufferSize;

	private AvroMessageEncoderDecoder avroMessageDecoder;
	private static final String SCHEMA_FILE_NAME = "schemaFileName";
	private static final String BUFFER_SIZE = "buffer_size";

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();

		schemaFileName = PropertyHelper.getStringProperty(getPropertyMap(), SCHEMA_FILE_NAME);
		bufferSize = PropertyHelper.getIntProperty(getPropertyMap(), BUFFER_SIZE, 0);
		logger.debug("building AvroJsonMapperHandler", "schemaFileName={} buffer_size", schemaFileName, bufferSize);
		avroMessageDecoder = new AvroMessageEncoderDecoder(schemaFileName, bufferSize);
		logger.debug("building handler", "building avro handler");
	}

	/**
	 * Picks up the events from handler context and processes them. If there is
	 * no data in the list set on context, an {@link IllegalArgumentException}
	 * is thrown.
	 * 
	 * @formatter:off
	 * if (anything in journal)
	 * 	process from journal
	 * else 
	 * 	process from context
	 * 
	 * 
	 * @return {@link Status#READY} if there was only one event in the input
	 *         list. Returns {@link Status#CALLBACK} if there were more than one
	 *         events available in the list.
	 * @throws HandlerException
	 * 
	 * @formatter:on
	 */
	@Override
	public Status process() throws HandlerException {
		logger.debug("processing handler", "processing avro handler");
		if (getSimpleJournal().getEventList() != null && !getSimpleJournal().getEventList().isEmpty()) {
			// process for CALLBACK status.
			/*
			 * @formatter:off
			 * Get the list from journal
			 * remove one from the list
			 * submit to channel if needed
			 * set one in context
			 * if more available in journal list, return CALLBACK
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getSimpleJournal().getEventList();
			logger.debug("process AvroJsonMapperHandler", "_message=\"journal not empty\" list_size={}",
					actionEvents.size());
			return processIt(actionEvents);

		} else {
			// process for ready status.
			/*
			 * @formatter:off
			 * Get the list from context
			 * remove one from the list
			 * submit to channel if needed
			 * set one in context
			 * if more available in context list, return CALLBACK
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			logger.debug("process AvroJsonMapperHandler",
					"_message=\"journal empty, will process from context\" actionEvents={}", actionEvents);

			Preconditions.checkNotNull(actionEvents);
			Preconditions.checkArgument(!actionEvents.isEmpty(),
					"eventList in HandlerContext must contain at least one ActionEvent");
			return processIt(actionEvents);
		}
	}

	private Status processIt(List<ActionEvent> actionEvents) throws HandlerException {
		Status statusToReturn = Status.READY;
		try {
			ActionEvent actionEvent = actionEvents.remove(0);
			byte[] avroDocument = actionEvent.getBody();
			JsonNode jsonDocument = avroMessageDecoder.decode(avroDocument);
			actionEvent.setBody(jsonDocument.toString().getBytes(Charset.defaultCharset()));
			/*
			 * Check for outputChannel map. get the eventList of channels. check
			 * the criteria and put the message.
			 */
			if (getOutputChannel() != null) {
				getOutputChannel().put(actionEvent);
			}
			getHandlerContext().createSingleItemEventList(actionEvent);
			if (!actionEvents.isEmpty()) {
				getSimpleJournal().setEventList(actionEvents);
				statusToReturn = Status.CALLBACK;
			} else {
				getSimpleJournal().setEventList(null);
				statusToReturn = Status.READY;
			}
		} catch (IOException e) {
			throw new HandlerException("unable to decode the avro document", e);
		}
		return statusToReturn;

	}

	private HandlerJournal getSimpleJournal() throws HandlerException {
		HandlerJournal simpleJournal = getNonNullJournal(SimpleJournal.class);// getJournal();
																				// =
																				// null;
		// Object simpleJournalObj = getJournal();
		// if (simpleJournalObj != null)
		// simpleJournal = ((HandlerJournal) simpleJournalObj);
		// else {
		// simpleJournal = new SimpleJournal();
		// getHandlerContext().setJournal(getId(), simpleJournal);
		// }
		return simpleJournal;
	}

}
