/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.CollectionUtil;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.HandlerConfigConstants;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

/**
 * Abstract implementation of Handler.
 *
 * @author Neeraj Jain
 */

public abstract class AbstractHandler implements Handler {
    private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AbstractHandler.class));
    private final String id = UUID.randomUUID().toString();
    private State state;
    /**
     * Name of this handler.
     */
    private String name;
    private int index;

    /**
     * Invocation count for this handler.
     */
    private long invocationCount;

    /**
     * Properties set for this handler.
     */
    private Map<String, Object> propertyMap;

    /**
     * Map from input to outputChannel, defines which outputChannel the data from an input should be sent to.
     *
     * @formatter:off e.g. "outputChannel-map" : "input1:channel1, input2:channel2",
     * means input1's data should be sent to channel1 and input2's data should be sent to channel2.
     * @formatter:on
     */
    private DataChannel outputChannel;

    private String handlerPhase = "";

    private boolean lastHandler = false;

    private Class<? extends Handler> clazz = null;

    private String[] getInputChannelArray(String channelMapValue) {
        return channelMapValue.split(","); // spilt "input1:channel1,
        // input2:channel2"
    }

    @Override
    public void build() throws AdaptorConfigurationException {
        if (StringHelper.isBlank(handlerPhase)) {
            setHandlerPhase("building " + getName());
        }
        logger.info(getHandlerPhase(),
                "handler_index=\"{}\" handler_name=\"{}\" properties=\"{}\" properties_class={} properties_hashCode={}",
                getIndex(), getName(), getPropertyMap(), getPropertyMap().getClass(), getPropertyMap().hashCode());
        @SuppressWarnings("unchecked")
        Entry<String, String> srcDesc = (Entry<String, String>) getPropertyMap().get(SourceConfigConstants.SRC_DESC);
        logger.info(getHandlerPhase(), "handler_name=\"{}\" \"src_desc\"=\"{}\" handler=\"{}\"", getName(), srcDesc,
                this.getId());
        if (srcDesc != null) {
            String srcDescInputName = srcDesc.getValue();
            if (getPropertyMap().containsKey(HandlerConfigConstants.CHANNEL_MAP)) {
                String channelMapValue = getPropertyMap().get(HandlerConfigConstants.CHANNEL_MAP).toString();
                logger.debug(getHandlerPhase(), "handler_name=\"{}\" channel_map=\"{}\"", getName(), channelMapValue);
                String[] inputChannelArray = getInputChannelArray(channelMapValue);

                final Map<String, DataChannel> channelMap = AdaptorConfig.getInstance().getAdaptorContext()
                        .getChannelMap();

                for (String inputChannelValue : inputChannelArray) {
                    String[] inputChannelValuesMap = inputChannelValue.split(":");
                    if (inputChannelValuesMap.length != 2) {
                        throw new AdaptorConfigurationException("value must be in input:channel format");
                    }
                    String inputName = inputChannelValuesMap[0].trim();
                    if (!inputName.equals(srcDescInputName)) {
                        continue;
                    }
                    setOutputChannel(channelMap.get(inputChannelValuesMap[1].trim()));
                    if (getOutputChannel() == null) {
                        throw new AdaptorConfigurationException(
                                "invalid value of outputChannel, outputChannel with name=" + inputName + " not found");
                    }
                    break;
                }

                if (outputChannel == null) {
                    throw new AdaptorConfigurationException("no channel mapped for input=" + srcDescInputName);
                }
                logger.debug("building handler", "handler_name=\"{}\" src_desc=\"{}\" outputChannel=\"{}\"", getName(),
                        srcDescInputName, outputChannel.getName());
            }
        }
        lastHandler = PropertyHelper.getBooleanProperty(getPropertyMap(),
                ActionEventHeaderConstants.LAST_HANDLER_IN_CHAIN);
        logger.debug(getHandlerPhase(), "lastHandler=\"{}\"", lastHandler);
    }

    @Override
    public String getId() {
        return id;
    }

    protected boolean setState(final State state) {
        if (this.state == state) {
            return false;
        }
        this.state = state;
        return true;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void shutdown() {
        state = State.TERMINATED;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Create a new Map backed by. The input map maybe an instance of
     * UnmodifiableMap.
     */
    @Override
    public void setPropertyMap(Map<String, Object> propertyMap) {
        this.propertyMap = new HashMap<>(propertyMap);
    }

    protected Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public DataChannel getOutputChannel() {
        return outputChannel;
    }

    protected void setOutputChannel(DataChannel outputChannel) {
        this.outputChannel = outputChannel;
    }

    /**
     * @param runtimeInfoStore     store to get from and save to runtime information
     * @param entityName           entity that the runtime information belongs to
     * @param availableDescriptors list of descriptors(files, tables etc) that are available to
     *                             this handler
     * @param inputDescriptor      descriptor
     * @return
     * @throws RuntimeInfoStoreException
     */

    protected <T> T getNextDescriptorToProcess(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                               final String entityName, List<T> availableDescriptors, final InputDescriptor<T> inputDescriptor)
            throws RuntimeInfoStoreException {
        final List<RuntimeInfo> runtimeInfos = runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
                entityName);
        logger.debug(getHandlerPhase(), "runtimeInfos=\"{}\"", runtimeInfos);

        final RuntimeInfo runtimeInfo = runtimeInfoStore.getLatest(AdaptorConfig.getInstance().getName(), entityName);
        logger.debug(getHandlerPhase(), "latestRuntimeInfo=\"{}\"", runtimeInfo);
        T nextDescriptorToProcess = null;
        if (runtimeInfo == null) {
            nextDescriptorToProcess = availableDescriptors.get(0);
        } else {
            logger.debug(getHandlerPhase(), "handler_id=\"{}\" latestRuntimeInfo=\"{}\"", getId(), runtimeInfo);
            String lastRuntimeInfoDescriptor = runtimeInfo.getInputDescriptor();
            nextDescriptorToProcess = inputDescriptor.getNext(availableDescriptors, lastRuntimeInfoDescriptor);
            logger.debug(getHandlerPhase(), "computed nextDescriptorToProcess=\"{}\"", nextDescriptorToProcess);
        }
        return nextDescriptorToProcess;
    }

    /**
     * @param runtimeInfoStore
     * @param entityName
     * @param inputDescriptorPrefix
     * @return
     * @throws RuntimeInfoStoreException
     */
    protected RuntimeInfo getOneQueuedRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                  final String entityName, final String inputDescriptorPrefix) throws RuntimeInfoStoreException {
        final List<RuntimeInfo> runtimeInfos = runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
                entityName, RuntimeInfoStore.Status.QUEUED);
        logger.debug(getHandlerPhase(), "found queued_runtimeInfos=\"{}\"",
                (runtimeInfos != null && !runtimeInfos.isEmpty()));
        if (runtimeInfos != null && !runtimeInfos.isEmpty()) {
            if (StringHelper.isBlank(inputDescriptorPrefix)) {
                return runtimeInfos.get(0);
            }
            for (RuntimeInfo runtimeInfo : runtimeInfos) {
                if (runtimeInfo.getInputDescriptor().startsWith(inputDescriptorPrefix))
                    return runtimeInfo;
            }
        }
        return null;
    }

    protected RuntimeInfo getOneQueuedRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                  final String entityName) throws RuntimeInfoStoreException {
        return getOneQueuedRuntimeInfo(runtimeInfoStore, entityName, null);
    }


    protected List<RuntimeInfo> getAllRuntimeInfos(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                   final String entityName, final String inputDescriptorPrefix, final Status status) throws RuntimeInfoStoreException {
        final List<RuntimeInfo> runtimeInfos = runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
                entityName, status);
        logger.debug(getHandlerPhase(), "all_runtimeInfos.size=\"{}\"", CollectionUtil.getSize(runtimeInfos));

        if (!StringHelper.isBlank(inputDescriptorPrefix) && CollectionUtil.isNotEmpty(runtimeInfos)) {
            final Iterator<RuntimeInfo> runtimeInfosIter = runtimeInfos.iterator();
            while (runtimeInfosIter.hasNext()) {
                final RuntimeInfo runtimeInfo = runtimeInfosIter.next();

                if (!runtimeInfo.getInputDescriptor().startsWith(inputDescriptorPrefix)) {
                    logger.debug(getHandlerPhase(),
                            "_message=\"removing from record list\" handler_id={} input_descriptor={} startWith={}",
                            getId(), runtimeInfo.getInputDescriptor(),
                            runtimeInfo.getInputDescriptor().startsWith(inputDescriptorPrefix));
                    runtimeInfosIter.remove();
                }
            }
        }
        return runtimeInfos;
    }


    protected List<RuntimeInfo> getAllStartedRuntimeInfos(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                          final String entityName, final String inputDescriptorPrefix) throws RuntimeInfoStoreException {
        return getAllRuntimeInfos(runtimeInfoStore, entityName, inputDescriptorPrefix, Status.STARTED);
    }

    protected List<RuntimeInfo> getAllRuntimeInfos(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                   final String entityName, final String inputDescriptorPrefix) throws RuntimeInfoStoreException {
        return runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), entityName, inputDescriptorPrefix);
    }

    protected List<RuntimeInfo> getAllStartedRuntimeInfos(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                          final String entityName) throws RuntimeInfoStoreException {

        return getAllStartedRuntimeInfos(runtimeInfoStore, entityName, null);
    }

    /**
     * Update RuntimeInfoStore with the status as Started.
     *
     * @param runtimeInfoStore
     * @param entityName
     * @param inputDescriptor
     * @return true if the update was successful, false otherwise
     * @throws RuntimeInfoStoreException
     */
    protected <T> boolean addRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore, final String entityName,
                                         final String inputDescriptor) throws RuntimeInfoStoreException {
        return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, RuntimeInfoStore.Status.STARTED);
    }

    protected <T> boolean queueRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                           final String entityName, final String inputDescriptor) throws RuntimeInfoStoreException {
        if (runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName, inputDescriptor) == null) {
            logger.info(getHandlerPhase(), "queueing adaptorName=\"{}\" entityName={} inputDescriptor={}",
                    AdaptorConfig.getInstance().getName(), entityName, inputDescriptor);
            return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, RuntimeInfoStore.Status.QUEUED);
        } else {
            logger.debug(getHandlerPhase(), "already in progress entityName={} inputDescriptor={}",
                    AdaptorConfig.getInstance().getName(), entityName, inputDescriptor);

        }
        return false;
    }

    /**
     * This method is used by handlers that have a complex inputDescriptor, one
     * which has string and properties.
     *
     * @param runtimeInfoStore
     * @param entityName
     * @param runtimeInfo
     * @return
     * @throws RuntimeInfoStoreException
     */
    protected <T> boolean queueRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                           final String entityName, final String inputDescriptor, final Map<String, String> properties)
            throws RuntimeInfoStoreException {
        if (runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName, inputDescriptor) == null) {
            logger.info(getHandlerPhase(), "queueing adaptorName=\"{}\" entityName={} inputDescriptor={}",
                    AdaptorConfig.getInstance().getName(), entityName, inputDescriptor);
            return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, RuntimeInfoStore.Status.QUEUED,
                    properties);
        } else {
            logger.debug(getHandlerPhase(), "already in progress, adaptorName=\"{}\" entityName={} inputDescriptor={}",
                    AdaptorConfig.getInstance().getName(), entityName, inputDescriptor, properties);

        }
        return false;
    }

    protected <T> boolean updateRuntimeInfoToStoreAfterValidation(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                                                  boolean validationPassed, ActionEvent actionEvent) throws RuntimeInfoStoreException {

        String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME);

        String inputDescriptor = actionEvent.getHeaders().get(ActionEventHeaderConstants.FULL_DESCRIPTOR);
        if (inputDescriptor == null)
            inputDescriptor = actionEvent.getHeaders().get(ActionEventHeaderConstants.INPUT_DESCRIPTOR);
        Map<String, String> properties = actionEvent.getHeaders();
        if (validationPassed) {
            return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, Status.VALIDATED, properties);
        } else {
            return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, Status.FAILED, properties);
        }
    }

    protected <T> boolean updateRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                            final String entityName, final String inputDescriptor, RuntimeInfoStore.Status status)
            throws RuntimeInfoStoreException {
        return updateRuntimeInfo(runtimeInfoStore, entityName, inputDescriptor, status, null);
    }

    protected <T> boolean updateRuntimeInfo(final RuntimeInfoStore<RuntimeInfo> runtimeInfoStore,
                                            final String entityName, final String inputDescriptor, RuntimeInfoStore.Status status,
                                            Map<String, String> properties) throws RuntimeInfoStoreException {
        RuntimeInfo startingRuntimeInfo = new RuntimeInfo();
        startingRuntimeInfo.setAdaptorName(AdaptorConfig.getInstance().getName());
        startingRuntimeInfo.setEntityName(entityName);
        startingRuntimeInfo.setInputDescriptor(inputDescriptor);
        startingRuntimeInfo.setStatus(status);
        startingRuntimeInfo.setProperties(properties);
        logger.debug(getHandlerPhase(),
                "_message=\"updating runtime info store, calling put\" inputDescriptor={} status={} startingRuntimeInfo={}",
                inputDescriptor, status, startingRuntimeInfo);

        boolean updated = runtimeInfoStore.put(startingRuntimeInfo);
        RuntimeInfo afterUpdate = runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName,
                inputDescriptor);
        logger.debug(getHandlerPhase(),
                "_message=\"updating runtime info store, calling put\" inputDescriptor={} status={} startingRuntimeInfo={} afterUpdate={} updated={}",
                inputDescriptor, status, startingRuntimeInfo, afterUpdate, updated);
        return updated;

    }

    /**
     * Get the context from ThreadLocal.
     *
     * @return
     */
    protected HandlerContext getHandlerContext() {
        return HandlerContext.get();
    }

    /**
     * Get the journal for this handler.
     *
     * @return
     */
    // protected Object getJournal() {
    // return getHandlerContext().getJournal(getId());
    // }
    @SuppressWarnings("unchecked")
    protected <T extends HandlerJournal> T getJournal(Class<T> clazz) throws HandlerException {
        return (T) getHandlerContext().getJournal(getId());
    }

    protected <T extends HandlerJournal> T getNonNullJournal(Class<T> clazz) throws HandlerException {
        // return (T) getHandlerContext().getJournal(getId());
        T journal = getJournal(clazz);
        if (journal == null) {
            try {
                journal = clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException ex) {
                throw new HandlerException(ex);
            }
        }
        getHandlerContext().setJournal(getId(), journal);
        return journal;
    }

    /**
     * Set the journal for this handler.
     *
     * @param journal
     */
    protected void setJournal(HandlerJournal journal) {
        getHandlerContext().setJournal(getId(), journal);
    }

    protected void setHandlerPhase(final String _handlerPhase) {
        handlerPhase = _handlerPhase;
    }

    protected String getHandlerPhase() {
        return handlerPhase;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractHandler other = (AbstractHandler) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public void handleException() {
        // default implementation does nothing.
    }

    @Override
    public void setIndex(final int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    public long getInvocationCount() {
        return invocationCount;
    }

    public void incrementInvocationCount() {
        this.invocationCount++;
    }

    public boolean isFirstRun() {
        return getInvocationCount() == 1;
    }

    public String getEntityNameFromHeader() {
        List<ActionEvent> eventList = getHandlerContext().getEventList();
        if (eventList != null && !eventList.isEmpty()) {
            return eventList.get(0).getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME);
        }
        return null;
    }

    public int getParentRuntimeIdFromHeader() {
        List<ActionEvent> eventList = getHandlerContext().getEventList();
        if (eventList != null && !eventList.isEmpty()) {
            String runtimeIdStr = eventList.get(0).getHeaders().get(ActionEventHeaderConstants.PARENT_RUNTIME_ID);
            if (runtimeIdStr != null) {
                return Integer.valueOf(runtimeIdStr);
            }
        }
        return -1;
    }

    @Override
    public io.bigdime.core.ActionEvent.Status process() throws HandlerException {
        setHandlerPhase("processing " + getName());
        incrementInvocationCount();
        logger.debug(getHandlerPhase(), "_messagge=\"entering process\" invocation_count={}", getInvocationCount());
        try {
            io.bigdime.core.ActionEvent.Status status = preProcess();
            if (status == io.bigdime.core.ActionEvent.Status.BACKOFF) {
                logger.debug(getHandlerPhase(), "returning BACKOFF");
                return status;
            }
            return doProcess();
        } catch (IOException e) {
            logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
                    "error during process", e);
            throw new HandlerException("Unable to process", e);
        } catch (RuntimeInfoStoreException e) {
            throw new HandlerException("Unable to process", e);
        }
    }

    protected io.bigdime.core.ActionEvent.Status preProcess()
            throws IOException, RuntimeInfoStoreException, HandlerException {
        return io.bigdime.core.ActionEvent.Status.READY;
    }

    protected io.bigdime.core.ActionEvent.Status doProcess()
            throws IOException, RuntimeInfoStoreException, HandlerException {
        return io.bigdime.core.ActionEvent.Status.READY;
    }

    protected void processLastHandler() {
        if (lastHandler) {
            logger.debug(getHandlerPhase(), "processing last handler's rituals");
            getHandlerContext().setEventList(null);
        }
    }

    /**
     * Class object of the class that implements Handler interface.
     */
    public void setHandlerClass(final Class<? extends Handler> _clazz) {
        this.clazz = _clazz;
    }

    public String getHandlerClass() {
        return clazz.getName();
    }
}