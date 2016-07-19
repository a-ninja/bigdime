package io.bigdime.handler.swift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;

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
	private String handlerPhase = "building SwiftByteWriterHandler";

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
			int eventsToWrite = 0;
			ActionEvent actionEventToWrite = null;
			for (ActionEvent actionEvent : actionEvents) {
				eventsToWrite++;
				logger.debug(handlerPhase, "headers={} uploadObjectType={}", actionEvent.getHeaders());

				String readCompleteStr = actionEvent.getHeaders().get(ActionEventHeaderConstants.READ_COMPLETE);

				if (!StringUtils.isBlank(readCompleteStr) && Boolean.parseBoolean(readCompleteStr) == true) {
					writeReady = true;
					actionEventToWrite = actionEvent;
					break;
				}
			}

			if (writeReady) {
				writeToSwift(actionEventToWrite, actionEvents, eventsToWrite);
			}
			if (!actionEvents.isEmpty()) {
				journal.setEventList(actionEvents);
			}
			journal.setEventList(actionEvents);
		} catch (Exception e) {
			throw new HandlerException(e.getMessage(), e);
		}
		long endTime = System.currentTimeMillis();
		logger.debug(handlerPhase, "statusToReturn={}", statusToReturn);
		logger.info(handlerPhase, "SwiftWriter finished in {} milliseconds", (endTime - startTime));
		return statusToReturn;

	}

	private void writeToSwift(final ActionEvent actionEvent, List<ActionEvent> actionEvents,
			final int eventsToWrite) throws IOException, HandlerException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			String fileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
			String swiftObjectName = outputFilePathPattern;
			Matcher m = inputPattern.matcher(fileName);

			while (m.find()) {
				logger.debug(handlerPhase, "_message=\"matched filename\" filename={}", m.group());
				String key = null;

				for (int i = 1; i <= m.groupCount(); i++) {
					key = "$" + i;
					String temp = m.group(i);
					logger.debug(handlerPhase, "file-part={}", temp);
					swiftObjectName = swiftObjectName.replace(key, temp);
					logger.debug(handlerPhase, "objectName={}", swiftObjectName);
				}
				logger.debug(handlerPhase, "final objectName={}", swiftObjectName);

			}
			for (int i = 0; i < eventsToWrite; i++) {
				baos.write(actionEvents.remove(0).getBody());
			}

			logger.debug(handlerPhase, "_message=\"writing to swift\" swiftObjectName={}", swiftObjectName);
			uploadBytes(container, swiftObjectName, baos.toByteArray());

		} finally {
			try {
				baos.close();
			} catch (IOException e) {
				logger.warn(handlerPhase, "exception while trying to close the ByteArrayOutputStream", e);
				// duck
			}
		}
	}

	private void uploadBytes(final Container container, final String objectName, final byte[] data) {
		StoredObject object = container.getObject(objectName);
		object.uploadObject(data);
	}

}
