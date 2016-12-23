package io.bigdime.handler.swift;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;

/**
 * @author Neeraj Jain
 */

@Component
@Scope("prototype")
public final class SwiftFileWriterHandler extends SwiftWriterHandler {
    private static final AdaptorLogger logger = new AdaptorLogger(
            LoggerFactory.getLogger(SwiftFileWriterHandler.class));

    @Override
    public Status process() throws HandlerException {
        setHandlerPhase("processing SwiftFileWriterHandler");
        return super.process();
    }

    protected Status process0(List<ActionEvent> actionEvents) throws HandlerException {
        long startTime = System.currentTimeMillis();
        SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
        if (journal == null) {
            logger.debug(getHandlerPhase(), "jounral is null, initializing");
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
        logger.debug(getHandlerPhase(), "statusToReturn={}", statusToReturn);
        logger.info(getHandlerPhase(), "SwiftFileWriterHandler finished in {} milliseconds", (endTime - startTime));
        return statusToReturn;

    }

    private void writeToSwift(final ActionEvent actionEvent) throws IOException, HandlerException {

        String fileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
        String swiftObjectName = StringHelper.replaceTokens(fileName, outputFilePathPattern, inputPattern,
                actionEvent.getHeaders());
        swiftClient.write(swiftObjectName, new String(fileName));
    }
}
