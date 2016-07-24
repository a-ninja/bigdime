package io.bigdime.handler.swift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
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
public class SwiftByteWriterHandler extends SwiftWriterHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(SwiftByteWriterHandler.class));
	private String handlerPhase = "processing SwiftByteWriterHandler";

	@Override
	protected Status process0(List<ActionEvent> actionEvents) throws HandlerException {

		SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
		if (journal == null) {
			logger.debug(handlerPhase, "jounral is null, initializing");
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
					logger.debug(handlerPhase, "_message=\"found readComplete\" headers={} eventsToWrite={}",
							actionEvent.getHeaders(), numEventsToWrite);
					break;
				}
			}

			// If writeReady, write to swift and clear those events from list.
			if (writeReady) {
				logger.debug(handlerPhase, "_message=\"calling writeToSwift\" numEventsToWrite={}", numEventsToWrite);
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
		logger.debug(handlerPhase, "statusToReturn={}", statusToReturn);
		logger.info(handlerPhase, "SwiftWriter finished in {} milliseconds", (endTime - startTime));
		return statusToReturn;

	}

	private ActionEvent writeToSwift(final ActionEvent actionEvent, List<ActionEvent> actionEvents)
			throws IOException, HandlerException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			String fileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
			String swiftObjectName = computeSwiftObjectName(fileName, outputFilePathPattern, inputPattern);
			long sizeToWrite = 0;
			// long count = 0;
			for (final ActionEvent thisEvent : actionEvents) {
				// count++;
				sizeToWrite = sizeToWrite + thisEvent.getBody().length;
				// logger.debug(handlerPhase, "_message=\"before write to
				// swift\" sizeToWrite={} count={}", sizeToWrite,
				// count);
				baos.write(thisEvent.getBody());
			}

			actionEvents.clear();
			byte[] dataToWrite = baos.toByteArray();
			baos.close();
			logger.debug(handlerPhase, "_message=\"writing to swift\" swift_object_name={} object_length={}",
					swiftObjectName, dataToWrite.length);
			StoredObject object = uploadBytes(container, swiftObjectName, dataToWrite);
			final ActionEvent outputEvent = new ActionEvent();
			outputEvent.setHeaders(actionEvent.getHeaders());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SwiftHeaders.OBJECT_NAME, object.getName());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SwiftHeaders.OBJECT_ETAG, object.getEtag());
			setOutputEventHeaders(outputEvent);
			outputEvent.setBody(dataToWrite);
			return outputEvent;
		} finally {
			try {
				baos.close();
			} catch (IOException e) {
				logger.warn(handlerPhase, "exception while trying to close the ByteArrayOutputStream", e);
				// duck
			}
		}
	}

	private StoredObject uploadBytes(final Container container, final String objectName, final byte[] data) {
		StoredObject object = container.getObject(objectName);
		object.uploadObject(data);
		logger.debug(handlerPhase,
				"_message=\"wrote to swift\" swift_object_name={} object_etag={} object_public_url={}", objectName,
				object.getEtag(), object.getPublicURL());
		return object;
	}

}
