package io.bigdime.handler.swift;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;

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
public class SwiftFileWriterHandler extends SwiftWriterHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(SwiftFileWriterHandler.class));
	private String handlerPhase = "building SwiftFileWriterHandler";

	protected Status process0(List<ActionEvent> actionEvents) throws HandlerException {
		long startTime = System.currentTimeMillis();
		SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
		if (journal == null) {
			logger.debug(handlerPhase, "jounral is null, initializing");
			journal = new SwiftWriterHandlerJournal();
			getHandlerContext().setJournal(getId(), journal);
		}
		Status statusToReturn = Status.READY;

		try {

			ActionEvent actionEvent = actionEvents.remove(0);

			writeToSwift(actionEvent);
			if (!actionEvents.isEmpty()) {
				journal.setEventList(actionEvents);
			}
		} catch (Exception e) {
			throw new HandlerException(e.getMessage(), e);
		}
		long endTime = System.currentTimeMillis();
		logger.debug(handlerPhase, "statusToReturn={}", statusToReturn);
		logger.info(handlerPhase, "SwiftFileWriterHandler finished in {} milliseconds", (endTime - startTime));
		return statusToReturn;

	}

	private void writeToSwift(final ActionEvent actionEvent) throws IOException, HandlerException {

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
		uploadFile(container, swiftObjectName, new String(fileName));
	}

	private void uploadFile(final Container container, final String objectName, final String fileName) {
		StoredObject object = container.getObject(objectName);
		object.uploadObject(new File(fileName));
	}
}
