package io.bigdime.handler.swift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;

public abstract class SwiftAbstractByteWriterHandler extends SwiftWriterHandler {
    private static final AdaptorLogger logger = new AdaptorLogger(
            LoggerFactory.getLogger(SwiftAbstractByteWriterHandler.class));

    protected ActionEvent writeToSwift(final ActionEvent actionEvent, List<ActionEvent> actionEvents)
            throws IOException, HandlerException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            String fileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
            String swiftObjectName = StringHelper.replaceTokens(fileName, outputFilePathPattern, inputPattern,
                    actionEvent.getHeaders());

            long sizeToWrite = 0;
            // long count = 0;
            for (final ActionEvent thisEvent : actionEvents) {
                // count++;
                sizeToWrite = sizeToWrite + thisEvent.getBody().length;
                // logger.debug(getHandlerPhase(), "_message=\"before write to
                // swift\" sizeToWrite={} count={}", sizeToWrite,
                // count);
                baos.write(thisEvent.getBody());
            }

            actionEvents.clear();
            byte[] dataToWrite = baos.toByteArray();
            baos.close();
            logger.info(getHandlerPhase(), "_message=\"writing to swift\" swift_object_name={} object_length={}",
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
                logger.warn(getHandlerPhase(), "exception while trying to close the ByteArrayOutputStream", e);
                // duck
            }
        }
    }

    protected StoredObject uploadBytes(final Container container, final String objectName, final byte[] data) {
        StoredObject object = container.getObject(objectName);
        boolean uploaded = false;
        do {
            try {
                object.uploadObject(data);
                uploaded = true;
            } catch (Exception ex) {
                logger.warn(getHandlerPhase(), "exception while uploading data to Swift");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }
            }
        } while (!uploaded);
        setExpiry(object, objectName);
        logger.info(getHandlerPhase(),
                "_message=\"wrote to swift\" swift_object_name={} object_etag={} object_public_url={}", objectName,
                object.getEtag(), object.getPublicURL());
        return object;
    }

    protected void setExpiry(final StoredObject object, final String objectName) {
        try {
            object.setDeleteAfter(TimeUnit.DAYS.toSeconds(14));
            logger.debug(getHandlerPhase(),
                    "_message=\"set expiry date\" swift_object_name={} object_etag={} object_public_url={}", objectName,
                    object.getEtag(), object.getPublicURL());

        } catch (final Exception ex) {
            logger.warn(getHandlerPhase(), "exception while trying to set the setDeleteAfter");
        }
    }
}