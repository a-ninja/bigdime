/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import java.util.Collection;
import java.util.HashSet;

import javax.annotation.PostConstruct;

import org.apache.flume.lifecycle.LifecycleState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.AdaptorContext;
import io.bigdime.core.AdaptorPhase;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.Sink;
import io.bigdime.core.Source;
import io.bigdime.core.channel.ChannelFactory;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.ADAPTOR_TYPE;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigReader;
import io.bigdime.core.config.ChannelConfig;
import io.bigdime.core.config.SinkConfig;
import io.bigdime.core.sink.DataSink;
import io.bigdime.core.sink.DataSinkFactory;
import io.bigdime.core.source.DataSourceFactory;

/**
 * DataAdaptor aka adaptor driver builds the instance of the Adaptor by reading
 * the adaptor config. It also builds source, channels and sinks and controls
 * their lifecycle.
 *
 * @author Neeraj Jain
 */

@Component
public final class DataAdaptor implements Adaptor {
    private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataAdaptor.class));
    private static AdaptorPhase adaptorCurrentPhase = AdaptorPhase.INIT;

    @Autowired
    private AdaptorConfigReader adaptorConfigReader;
    @Autowired
    private ChannelFactory channelFactory;
    @Autowired
    private DataSinkFactory dataSinkFactory;
    @Autowired
    private DataSourceFactory dataSourceFactory;
    @Autowired
    private DataAdaptorJob dataAdaptorJob;
    private boolean adaptorStopped = false;
    private static long DEFAULT_SLEEP_DURATION_SECONDS = 30;
    private long heartbeatSleepDurationMillis = DEFAULT_SLEEP_DURATION_SECONDS * 1000;
    private Thread heartbeatThread = null;
    @Autowired
    private MetadataStore metadataStore;

    private AdaptorConfig config = AdaptorConfig.getInstance();

    /**
     * If any of the sources is not in stopped state, the sourceRunning flag is
     * set to true. This is used to ensure that if the source.start is invoked
     * only when sources are not already processing.
     */
    private boolean sourceRunning;
    private boolean sinkRunning;

    /**
     * Keep the constructor private, only one instance of Adaptor is needed per
     * runtime so this needs to be a singleton.
     *
     * @throws AdaptorConfigurationException
     */
    private DataAdaptor() throws AdaptorConfigurationException {
    }

    @PostConstruct
    private void init() throws AdaptorConfigurationException {
        try {
            // Set the phase, needed for the logging and debugging puroposes.
            adaptorCurrentPhase = AdaptorPhase.INIT;
            logger.info(adaptorCurrentPhase.getValue(), "initializing DataAdaptor");

            adaptorConfigReader.readConfig(config);

            logger.info(adaptorCurrentPhase.getValue(), config.toString());
            initializeComponents();
            if (config.isAutoStart()) {
                if (config.getType() == ADAPTOR_TYPE.BATCH)
                    dataAdaptorJob.scheduleBatchJob(this);
                else
                    dataAdaptorJob.scheduleStreamingJob(this);
            }
            logger.info(adaptorCurrentPhase.getValue(), "source=\"{}\" channels=\"{}\" sinks=\"{}\"",
                    getSources().size(), getChannels().size(), getSinks().size());

            // register with metadata store.
            metadataStore.createDatasourceIfNotExist(AdaptorConfig.getInstance().getName(),
                    config.getSourceConfig().getSourceType());
        } catch (AdaptorConfigurationException ex) {
            logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
                    ALERT_SEVERITY.BLOCKER, ex.getMessage(), ex);
            throw ex;
        } catch (Exception ex) {
            logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
                    ALERT_SEVERITY.BLOCKER, ex.getMessage(), ex);
            Assert.isNull(ex, "Exception during DataAdaptor startup, can not continue");
        }
    }

    /**
     * Start the adaptor by starting the source handlers, channels and sink
     * handlers. First step is to create instances of channels and start them.
     * Second step is to create an instance of sink and start it so that it can
     * start consuming the events from channels if there are any pending events
     * from past runs. Third step is to create an instance of source and start
     * it.
     *
     * @throws DataAdaptorException
     */
    @Override
    public synchronized boolean start() throws DataAdaptorException {
        logger.info("starting adaptor", "command received to start adaptor");
        if (adaptorCurrentPhase == AdaptorPhase.STARTING || adaptorCurrentPhase == AdaptorPhase.STARTED) {
            boolean started = startSources();
            return started;
        } else {
            setAdaptorCurrentPhase(AdaptorPhase.STARTING);
            startChannels();
            startSink();
            startSources();
            setAdaptorCurrentPhase(AdaptorPhase.STARTED);
            checkHeartbeat();
        }
        return true;
    }

    @Override
    public synchronized void stop() throws DataAdaptorException {
        if (adaptorCurrentPhase != AdaptorPhase.STARTING && adaptorCurrentPhase != AdaptorPhase.STARTED) {
            throw new DataAdaptorException("adaptor not running");
        }
        setAdaptorCurrentPhase(AdaptorPhase.STOPPING);
        stopSource();
        stopSink();
        setAdaptorCurrentPhase(AdaptorPhase.STOPPED);
        adaptorStopped = true;
    }

    private static void setAdaptorCurrentPhase(AdaptorPhase adaptorPhase) {
        adaptorCurrentPhase = adaptorPhase;
    }

    public static String getAdaptorCurrentPhase() {
        return adaptorCurrentPhase.getValue();
    }

    private void initializeComponents() throws AdaptorConfigurationException {
        initializeChannels();
        initializeSource();
        initializeSinks();
    }

    private void initializeSource() throws AdaptorConfigurationException {
        setSources(dataSourceFactory.getDataSource(config.getSourceConfig()));
    }

    private void initializeChannels() throws AdaptorConfigurationException {
        Collection<DataChannel> channels = new HashSet<>();
        for (final ChannelConfig channelConfig : config.getChannelConfigs()) {
            final Collection<DataChannel> channel = channelFactory.getChannel(channelConfig);
            channels.addAll(channel);
        }
        if (channels.isEmpty()) {
            throw new AdaptorConfigurationException(
                    "adaptor could not be configured, channels is null after building the adaptor");
        }
        setChannels(channels);
    }

    private void initializeSinks() throws AdaptorConfigurationException {
        Collection<Sink> sinks = new HashSet<>();
        for (final SinkConfig sinkConfig : config.getSinkConfigs()) {
            final Collection<Sink> sink = dataSinkFactory.getDataSink(sinkConfig);
            sinks.addAll(sink);
        }
        for (Sink sink : sinks) {
            for (Source source : getSources())
                ((DataSink) sink).addObserver(source);
        }
        if (sinks.isEmpty()) {
            throw new AdaptorConfigurationException(
                    "adaptor could not be configured, sink is null after building the adaptor");
        }
        setSinks(sinks);
    }

    private void startChannels() throws AdaptorConfigurationException {
        logger.debug("adaptor calling start on each channel", "channels.size=\"{}\"", getChannels().size());
        for (final DataChannel channel : getChannels()) {
            logger.debug("adaptor calling start on channel", "channel_name=\"{}\"", channel.getName());
            channel.start();
        }
    }

    private boolean startSources() throws AdaptorConfigurationException {
        int numSourcesStarted = 0;
        sourceRunning = true;
        Collection<Source> sources = getSources();
        logger.debug("adaptor calling start on each source", "sources.size=\"{}\"", sources.size());
        for (final Source source : sources) {
            if (!(source.getLifecycleState() == LifecycleState.START)) {
                numSourcesStarted++;
                logger.debug("adaptor calling start on source", "source_name=\"{}\" count={}", source.getName(),
                        numSourcesStarted);
                source.start();
            } else {
                logger.info("starting adaptor",
                        "_message=\"source is still running. can't start the source\" source_name=\"{}\"",
                        source.getName());
            }
        }
        return (numSourcesStarted > 0);
    }

    private void startSink() throws AdaptorConfigurationException {
        logger.debug("adaptor calling start on each sink", "sinks_size=\"{}\"", getSinks().size());
        for (final Sink sink : getSinks()) {
            logger.debug("adaptor calling start on sink", "sink_name=\"{}\"", sink.getName());
            sink.start();
        }
    }

    private void checkHeartbeat() {
        heartbeatThread = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("healthcheck-DataAdaptor");
                    logger.info("heartbeat thread for DataAdaptor", "heathcheck thread for DataAdaptor");
                    while (!adaptorStopped) {
                        isSourceRunning();
                        isSinkRunning();
                        logger.debug("heartbeat thread for DataAdaptor", "source_running=\"{}\" sink_running=\"{}\"",
                                sourceRunning, sinkRunning);
                        sleep(heartbeatSleepDurationMillis);
                    }
                } catch (Exception e) {
                    logger.warn("heartbeat thread for DataAdaptor",
                            "DataAdaptor heartbeat thread received an exception, will duck it. sleep_duration=\"{}\"",
                            heartbeatSleepDurationMillis, e);
                }
            }
        };
        heartbeatThread.start();
    }

    /**
     * Checks the state of all the sources. If any of the sources is not in
     * stopped, true is returned.
     *
     * @return true if any if the sources is not in Stopped state, false
     * otherwise.
     */
    public boolean isSourceRunning() {
        sourceRunning = false;
        for (final Source source : getSources()) {
            LifecycleState state = source.getLifecycleState();
            if (state != LifecycleState.STOP)
                sourceRunning = true;
            logger.debug("DataAdaptor checking source state", "source_name=\"{}\" state=\"{}\"", source.getName(),
                    state);
        }
        return sourceRunning;
    }

    /**
     * Checks the state of all the sinks. If any of the sinks is not in stopped,
     * true is returned.
     *
     * @return true if any if the sinks is not in Stopped state, false
     * otherwise.
     */
    public boolean isSinkRunning() {
        sinkRunning = false;
        for (final Sink sink : getSinks()) {
            LifecycleState state = sink.getLifecycleState();
            if (state == LifecycleState.START)
                sinkRunning = true;
            logger.debug("DataAdaptor checking sink state", "sink_name=\"{}\" state=\"{}\"", sink.getName(), state);
        }
        return sinkRunning;
    }

    private void stopSource() throws AdaptorConfigurationException {
        for (final Source source : getSources()) {
            logger.debug("adaptor calling stop on source", "source_name=\"{}\"", source.getName());
            source.stop();
        }
    }

    private void stopSink() throws AdaptorConfigurationException {
        for (final Sink sink : getSinks()) {
            logger.debug("adaptor calling stop on sink", "sink_name=\"{}\"", sink.getName());
            sink.stop();
        }
    }

    /**
     * Get the application level context.
     */
    @Override
    public AdaptorContext getAdaptorContext() {
        return getAdaptorConfig().getAdaptorContext();
    }

    /**
     * Get adaptor config.
     */
    @Override
    public AdaptorConfig getAdaptorConfig() {
        return config;
    }

    public Collection<Source> getSources() {
        return config.getAdaptorContext().getSources();
    }

    public Collection<Sink> getSinks() {
        return config.getAdaptorContext().getSinks();
    }

    public Collection<DataChannel> getChannels() {
        return config.getAdaptorContext().getChannels();
    }

    public void setSources(Collection<Source> sources) {
        config.getAdaptorContext().setSources(sources);
    }

    public void setSinks(Collection<Sink> sinks) {
        config.getAdaptorContext().setSinks(sinks);
    }

    public void setChannels(Collection<DataChannel> channels) {
        config.getAdaptorContext().setChannels(channels);
    }
}
