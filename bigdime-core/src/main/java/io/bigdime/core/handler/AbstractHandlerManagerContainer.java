/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.HasHandlers;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.ADAPTOR_TYPE;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.AdaptorConstants;
import io.bigdime.util.RetriesExhaustedException;
import org.apache.flume.NamedComponent;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;

import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.*;

public abstract class AbstractHandlerManagerContainer implements NamedComponent, HasHandlers, LifecycleAware {
  private static final AdaptorLogger logger = new AdaptorLogger(
          LoggerFactory.getLogger(AbstractHandlerManagerContainer.class));
  private HandlerManager handlerManager;
  private LinkedHashSet<Handler> handlers;
  private String name;

  private boolean interrupted;

  private FutureTask<Object> futureTask;
  private ExecutorService executorService;
  private int errorCount = 0;

  private int errorThreshold = AdaptorConstants.ERROR_THRESHOLD;
  private long sleepForMillis = AdaptorConstants.SLEEP_WHILE_WAITING_FOR_DATA;

  private LifecycleState lifecycleState = LifecycleState.IDLE;
  private final String id = UUID.randomUUID().toString();

  protected abstract String getContainerType();// source or sink

  /**
   * Background thread that waits for future task to complete.
   */
  protected void startHealthcheckThread() {
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.currentThread().setName("healthcheck-" + AbstractHandlerManagerContainer.this.getName());
          logger.info("heathcheck thread for handlerManagerContainer",
                  "{}_name=\"{}\" handlerManager=\"{}\" thread_id={}", getContainerType(),
                  AbstractHandlerManagerContainer.this.getName(), getHandlerManager(),
                  Thread.currentThread().getId());
          futureTask.get();
          logger.info("heathcheck thread for handlerManagerContainer, future task completed",
                  "{}_name=\"{}\" handlerManager=\"{}\" futureTask.isDone=\"{}\" thread_id={}",
                  getContainerType(), AbstractHandlerManagerContainer.this.getName(), getHandlerManager(),
                  futureTask.isDone(), Thread.currentThread().getId());
          lifecycleState = LifecycleState.STOP;
        } catch (CancellationException e) {
          lifecycleState = LifecycleState.STOP;
        } catch (Exception e) {
          if (e.getCause() != null && e.getCause() instanceof RetriesExhaustedException) {
            RetriesExhaustedException rex = (RetriesExhaustedException) e.getCause();
            if (rex.causes() != null)
              logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                      ALERT_SEVERITY.BLOCKER, "causes=" + rex.causes().toString() + ", _message=\"task completed with an error\"", e);
            logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                    ALERT_SEVERITY.BLOCKER, "causes=" + rex.causes().toString() + ", _message=\"task completed with an error\"", e);
          } else
            logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                    ALERT_SEVERITY.BLOCKER, "_message=\"task completed with an error\"", e);
          lifecycleState = LifecycleState.ERROR;
        } finally {
          logger.info("heathcheck thread for handlerManagerContainer, shutting down executorService",
                  "{}_name=\"{}\" handlerManager=\"{}\" futureTask.isDone=\"{}\" thread_id={}",
                  getContainerType(), AbstractHandlerManagerContainer.this.getName(), getHandlerManager(),
                  futureTask.isDone(), Thread.currentThread().getId());
          executorService.shutdown();
        }
      }
    }.start();
    logger.info("started heathcheck thread for handlerManagerContainer",
            "{}_name=\"{}\" handlerManager=\"{}\" thread_id={}", getContainerType(), getName(), getHandlerManager(),
            Thread.currentThread().getId());
  }

  /**
   * Execute the handler chain in a new thread. Run the handler chain
   * continuously if the adaptor is of type streaming, otherwise run it just
   * once.
   */
  @Override
  public void start() {
    logger.debug("starting handlerManagerContainer", "{}_name=\"{}\" lifecycleState=\"{}\"", getContainerType(),
            getName(), lifecycleState);
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
        logger.debug("starting thread for handlerManagerContainer", "{}_name=\"{}\" adaptor_type=\"{}\"",
                getContainerType(), getName(), adaptorType);
        if (adaptorType == null) {
          logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
                  ALERT_SEVERITY.BLOCKER, "adaptor_type is null");
          throw new RuntimeException("invalid adaptor type, null not allowed");
        }
        switch (adaptorType) {
          case STREAMING:
            runForever();
            break;
          case BATCH:
            runOnce();
            break;
          default:
        }
        return null;
      }

    });
    startHealthcheckThread();
    executorService.execute(futureTask);
  }

  // TODO: handle NPE
  private void runOnce() throws HandlerException {
    Status status = executeHandlerChain();
    logger.debug("handler manager completed for runOnce", "{}_name=\"{}\" status=\"{}\"", getContainerType(),
            getName(), status);
  }

  // TODO: handle NPE
  private void runForever() throws HandlerException {
    while (!interrupted) {
      try {
        logger.debug("starting thread for handlerManagerContainer", "{}_name=\"{}\"", getContainerType(),
                getName());
        Status status = executeHandlerChain();
        logger.debug("handler manager completed for runForever", "{}_name=\"{}\" status=\"{}\"",
                getContainerType(), getName(), status);
        try {
          if (status == Status.BACKOFF)
            Thread.sleep(sleepForMillis);
        } catch (InterruptedException e) {
          logger.warn("handler chain returned", "thread interrupted while sleeping");
        }
      } catch (HandlerException e) {
        errorCount++;
        logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                ALERT_SEVERITY.BLOCKER, "handler chain threw an exception", e);
        if (errorCount > errorThreshold) {
          throw new HandlerException("unable to continue the adaptor, error count exceeded threshold");
        }
      }
    }
  }

  private Status executeHandlerChain() throws HandlerException {
    return getHandlerManager().execute();
  }

  /**
   * Stop the handlerManager chain
   */
  @Override
  public void stop() {
    logger.warn("stopping " + getContainerType(), "setting interrupted to true");
    interrupted = true;
    try {
      getHandlerManager().shutdown();
    } catch (IllegalHandlerStateException e) {
      // cant do much here, just continue with shutdown.
    }
    if (futureTask != null)
      futureTask.cancel(true);// send the interrupt
    executorService.shutdown();
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  protected void setLifecycleState(LifecycleState lifecycleState) {
    this.lifecycleState = lifecycleState;
  }

  /**
   * Name associated with this handlerManagerContainer
   */
  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String arg0) {
    this.name = arg0;
  }

  @Override
  public LinkedHashSet<Handler> getHandlers() {
    return handlers;
  }

  @Override
  public void setHandlers(LinkedHashSet<Handler> handlers) {
    this.handlers = handlers;
  }

  public HandlerManager getHandlerManager() {
    return handlerManager;
  }

  public void setHandlerManager(HandlerManager handlerManager) {
    this.handlerManager = handlerManager;
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
    AbstractHandlerManagerContainer other = (AbstractHandlerManagerContainer) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }
}
