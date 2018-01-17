/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.sink;

import java.util.LinkedHashSet;
import java.util.Observable;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.flume.lifecycle.LifecycleState;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.Sink;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.ADAPTOR_TYPE;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.AdaptorConstants;
import io.bigdime.core.handler.HandlerManager;
import io.bigdime.core.handler.IllegalHandlerStateException;

/**
 * Bigdime implementation of {@link Sink} interface. It uses HandlerManager to
 * manage the handler chain. DataSink extends Observable to allow Sources to
 * observe it. If the DataSink needs to be stopped for some reason, it notifies
 * Sources about the same so that Sources can take appropriate action, e.g. stop
 * or pause etc.
 *
 * @author Neeraj Jain
 */
public class DataSink extends Observable implements Sink {
    private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataSink.class));
    private final HandlerManager handlerManager;
    private LinkedHashSet<Handler> handlers;
    private String name;
    private String description;
    private long sleepForMillis = AdaptorConstants.SLEEP_WHILE_WAITING_FOR_DATA;
    private LifecycleState lifecycleState = LifecycleState.IDLE;
    private final String id = UUID.randomUUID().toString();

    /**
     * Creates a new <tt>DataSink</tt> given the handlers and the name.
     *
     * @param handlers set of handlers that will read and process the data
     * @param name     name of the data sink
     * @throws AdaptorConfigurationException if handler is null or empty
     */
    public DataSink(final LinkedHashSet<Handler> handlers, String name) throws AdaptorConfigurationException {
        if ((handlers == null) || handlers.isEmpty()) {
            logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
                    ALERT_SEVERITY.BLOCKER, "handlers not configured");
            throw new AdaptorConfigurationException("null or empty collection not allowed for handlers");
        }
        this.handlers = handlers;
        this.handlerManager = new HandlerManager(handlers, name);
        this.name = name;
    }

    @Override
    public String toString() {
        return "DataSink [handlers=" + handlers + ", name=" + name + ", description=" + description + "]";
    }

    @Override
    public LinkedHashSet<Handler> getHandlers() {
        return handlers;
    }

    @Override
    public void setHandlers(LinkedHashSet<Handler> handlers) {
        this.handlers = handlers;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    private boolean interrupted;

    private FutureTask<Object> futureTask;
    private ExecutorService executorService;
    private int errorCount = 0;

    private int errorThreshold = AdaptorConstants.ERROR_THRESHOLD;

    /**
     * Background thread that waits for future task to complete. If the future
     * task throws an exception, it notifies the Observers.
     */
    private void startHealthcheckThread() {
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("healthcheck-" + DataSink.this.getName());
                    logger.debug("heathcheck thread for sink", "sink_name=\"{}\" handlerManager=\"{}\"",
                            DataSink.this.getName(), handlerManager);
                    futureTask.get();
                    logger.info("heathcheck thread for sink, future task completed",
                            "sink_name=\"{}\" handlerManager=\"{}\" futureTask.isDone=\"{}\"", DataSink.this.getName(),
                            handlerManager, futureTask.isDone());
                    lifecycleState = LifecycleState.STOP;
                } catch (CancellationException e) {
                    logger.info("heathcheck thread for sink, future task cancelled",
                            "sink_name=\"{}\" handlerManager=\"{}\" futureTask.isDone=\"{}\"", DataSink.this.getName(),
                            handlerManager, futureTask.isDone());
                    lifecycleState = LifecycleState.STOP;
                } catch (Exception e) {
                    logger.debug("notifying observer", "observer_count=\"{}\"", countObservers());
                    lifecycleState = LifecycleState.ERROR;
                    setChanged();
                    notifyObservers(e);
                    // notifyDispatchers(e);
                    logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                            ALERT_SEVERITY.BLOCKER, "\"task completed with an exception\"", e);
                }
            }
        }.start();
        logger.info("started heathcheck thread for sink", "sink_name=\"{}\" handlerManager=\"{}\"",
                DataSink.this.getName(), handlerManager);
    }

    @Override
    public void start() {
        logger.debug("starting sink", "sink_name=\"{}\" handlerManager=\"{}\"", getName(), handlerManager);
        lifecycleState = LifecycleState.START;

        /**
         * Start the handlerManager chain
         */
        executorService = Executors.newSingleThreadExecutor();
        futureTask = new FutureTask<>(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                Thread.currentThread().setName(getName());
                final ADAPTOR_TYPE adaptorType = AdaptorConfig.getInstance().getType();
                logger.debug("starting sink thread", "sink_name=\"{}\" adaptor_type=\"{}\"", getName(), adaptorType);
                runForever();
                return null;
            }

        });
        startHealthcheckThread();
        executorService.execute(futureTask);
    }

    // TODO: handle NPE
    private void runForever() throws HandlerException {
        while (!interrupted) {
            try {
                logger.debug("starting sink thread", "sink_name=\"{}\"", getName());
                Status status = executeHandlerChain();
                postHandlerChain(status);
            } catch (HandlerException e) {
                errorCount++;
                logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                        ALERT_SEVERITY.BLOCKER,
                        "handler chain threw an exception, error_count=\"{}\" error_threshold=\"{}\"", errorCount,
                        errorThreshold, e);
                if (errorCount > errorThreshold) {
                    throw new HandlerException("unable to continue the adaptor, error count exceeded threshold");
                }
            }
        }
    }

    private Status executeHandlerChain() throws HandlerException {
        return handlerManager.execute();
    }

    private void postHandlerChain(Status status) {
        try {
            if (status == Status.BACKOFF)
                Thread.sleep(sleepForMillis);
        } catch (InterruptedException e) {
            logger.warn("data sink running", "thread interrupted while sleeping");
        }
    }

    @Override
    public void stop() {
        logger.warn("data sink stopping", "setting interrupted to true");
        interrupted = true;

        try {
            handlerManager.shutdown();
        } catch (IllegalHandlerStateException e) {
            // cant do much here, just continue with shutdown.
        }
        futureTask.cancel(true);// send the interrupt
        executorService.shutdown();
        lifecycleState = LifecycleState.STOP;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        DataSink other = (DataSink) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    // Collection<FailureEventDispatcher> failureEventDispatchers;
    //
    // @Override
    // public void addFailureEventDispatcher(FailureEventDispatcher
    // failureEventDispatcher) {
    // if (failureEventDispatchers == null)
    // failureEventDispatchers = new HashSet<>();
    // failureEventDispatchers.add(failureEventDispatcher);
    //
    // }
    //
    // @Override
    // public void notifyDispatchers(Object arg0) {
    // if (failureEventDispatchers != null) {
    // for (FailureEventDispatcher failureEventDispatcher :
    // failureEventDispatchers) {
    // failureEventDispatcher.dispatchFailureEvent(this, arg0);
    // }
    // }
    //
    // }
}
