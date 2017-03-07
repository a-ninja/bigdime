package io.bigdime.core.handler;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.CollectionUtil;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractSourceHandler extends AbstractHandler {
    private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AbstractSourceHandler.class));

    protected List<RuntimeInfo> dirtyRecords;
    @Autowired
    private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;
    private boolean processingDirty = false;

    public String getEntityName() {
        return "";
    }

    @Override
    public io.bigdime.core.ActionEvent.Status process() throws HandlerException {

        setHandlerPhase("processing " + getName());
        incrementInvocationCount();
        logger.debug(getHandlerPhase(), "_messagge=\"entering process\" invocation_count={}", getInvocationCount());
        try {
            init(); // initialize cleanup records etc
            initDescriptor();
            // if (isInputDescriptorNull()) {
            // logger.debug(getHandlerPhase(), "returning BACKOFF");
            // return io.bigdime.core.ActionEvent.Status.BACKOFF;
            // }
            return doProcess();
        } catch (IOException e) {
            logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
                    "error during process", e);
            throw new HandlerException("Unable to process", e);
        } catch (RuntimeInfoStoreException e) {
            throw new HandlerException("Unable to process", e);
        }
    }

    /**
     * Initialize handler.
     *
     * @throws RuntimeInfoStoreException
     */
    public void init() throws RuntimeInfoStoreException {
        if (isFirstRun()) {
            initClass();
            dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, getEntityName(), getInputDescriptorPrefix());
            if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
                logger.warn(getHandlerPhase(),
                        "_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entityName={}",
                        getId(), dirtyRecords.size(), getEntityName());
            } else {
                logger.info(getHandlerPhase(), "_message=\"no dirty records found\" handler_id={}", getId());
            }
        }
    }

    protected void initClass() throws RuntimeInfoStoreException {

    }

    protected void initDescriptor() throws HandlerException, RuntimeInfoStoreException {
        if (CollectionUtil.isNotEmpty(dirtyRecords)) {
            initDescriptorForCleanup();
        } else {
            initDescriptorForNormal();
        }
    }

    /**
     * For cleanup, just pick a record from available list of dirtyRecords
     * collection and set it up as the processing record.
     *
     * @return
     * @throws HandlerException
     */
    protected void initDescriptorForCleanup() throws HandlerException {
        final RuntimeInfo dirtyRecord = dirtyRecords.remove(0);
        logger.info(getHandlerPhase(), "_message=\"initializing a dirty record\" dirtyRecord=\"{}\"", dirtyRecord);
        // InputDescriptor<T> inputDescriptor =
        initRecordToProcess(dirtyRecord);
        processingDirty = true;

        // return inputDescriptor;
    }

    /**
     * For normal case, look for the next descriptor to process. It could be
     * queued record in the DB. If there is nothing in the DB, then look for a
     * new record which means look for a new file on disk or run a job for next
     * time window etc.
     *
     * @return
     * @throws RuntimeInfoStoreException
     * @throws HandlerException
     */
    public void initDescriptorForNormal() throws RuntimeInfoStoreException, HandlerException {
        logger.info(getHandlerPhase(), "initializing a clean record");
        processingDirty = false;
        // InputDescriptor<T> inputDescriptor = null;
        RuntimeInfo queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, getEntityName(),
                getInputDescriptorPrefix());
        logger.debug(getHandlerPhase(), "queued_record={}", queuedRecord);
        if (queuedRecord == null) {
            boolean foundRecordsToProcess = findAndAddRuntimeInfoRecords();
            if (foundRecordsToProcess)
                queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, getEntityName(), getInputDescriptorPrefix());
        }
        if (queuedRecord != null) {
            logger.info(getHandlerPhase(), "_message=\"found a queued record, will process this\" queued_record={}",
                    queuedRecord);
            initRecordToProcess(queuedRecord);
            Map<String, String> properties = new HashMap<>();
            properties.put("handlerName", getHandlerClass());
            updateRuntimeInfo(runtimeInfoStore, getEntityName(), queuedRecord.getInputDescriptor(),
                    RuntimeInfoStore.Status.STARTED, properties);
        } else {
            logger.debug(getHandlerPhase(), "no queued record found, setting descriptor to null");
            setInputDescriptorToNull();
        }
    }

    public List<RuntimeInfo> getDirtyRecords() {
        return dirtyRecords;
    }

    public boolean isProcessingDirty() {
        return processingDirty;
    }

    protected String getInputDescriptorPrefix() {
        return null;
    }

    protected void setInputDescriptorToNull() {
    }

    protected boolean isInputDescriptorNull() {
        return false;
    }

    public RuntimeInfoStore<RuntimeInfo> getRuntimeInfoStore() {
        return runtimeInfoStore;
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

    protected void initRecordToProcess(RuntimeInfo runtimeInfo) throws HandlerException {

    }

    protected boolean findAndAddRuntimeInfoRecords() throws RuntimeInfoStoreException, HandlerException {
        return true;
    }

}
