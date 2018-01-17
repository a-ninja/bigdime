/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.LinkedHashSet;
import java.util.Stack;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;

/**
 * Executes the chain of handlers by invoking process method on each handler of
 * the chain.
 * <p>
 * This class is NOT thread safe. If more than one threads invoke execute method
 * on same HandlerManager instance, the outcome could be unexpected.
 *
 * @author Neeraj Jain
 */
public class HandlerManager {
    private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HandlerManager.class));
    private LinkedHashSet<Handler> handlers;
    private HandlerNode handlerNodeHead;
    /**
     * Name of this handler chain.
     */
    private String handlerChainName;

    /**
     * Count of errors for this handler chain.
     */
    private int errorCount;
    private int thresholdErrorCount = 10;

    private int handlerChainExecutionCount;

    public HandlerManager(LinkedHashSet<Handler> handlers, String handlerChainName) {
        this.handlers = handlers;
        constructHandlerList();
        this.handlerChainName = handlerChainName;
        state = STATE.INITIALIZED;
    }

    private void constructHandlerList() {
        HandlerNode temp = null;
        if (handlers != null) {
            for (Handler h : handlers) {
                HandlerNode handlerNode = new HandlerNode(h);
                if (handlerNodeHead == null) {
                    handlerNodeHead = handlerNode;
                } else {
                    temp.setNext(handlerNode);
                }
                temp = handlerNode;
            }
        }
    }

    /**
     * List of handlers that this handler will manage.
     */

    /**
     * State transition : INITIALIZED->RUNNING->STOPPING->STOPPED
     *
     * @author Neeraj Jain
     */
    enum STATE {
        INITIALIZED,

        /**
         *
         */
        RUNNING,

        /**
         *
         */
        STOPPING,

        /**
         *
         */
        STOPPED
    }

    private STATE state;

    /**
     * Processes an ActionEvent through the chain of handlers. If the process
     * method of any handler in the chain throws an exception, an alert is
     * raised and the subsequent handlers are not processed.
     * <p>
     * Execute the handlers, run them in a do-while loop until the whole batch
     * is done.
     *
     * @param event
     * @return
     * @throws HandlerException if the process method of any handler throws an exception,
     *                          it's propagated to the caller of the execute method. Also if
     *                          the HandlerManager is already in RUNNING state, this method
     *                          throws an IllegalHandlerStateException indicating that the
     *                          handler chain could not be executed.
     */
    public Status execute() throws HandlerException {
        logger.debug("handler chain will execute",
                "_message=\"updating state to RUNNING\" handler_chain=\"{}\" current_state=\"{}\"", handlerChainName,
                state);
        /*
		 * @formatter:off
		 * update state to running.
		 * execute handlers.
		 * @formatter:on
		 */
        updateState(STATE.RUNNING);
        logger.debug("handler chain will execute", "_message=\"updated state to RUNNING\" handler_chain=\"{}\"",
                handlerChainName);

        Status status = execute0();
        logger.debug("handler chain executed successfully",
                "_message=\"updating state to STOPPED\" handler_chain=\"{}\"", handlerChainName);
        updateState(STATE.STOPPED);
        return status;
    }

    /*
     * @formatter:off
     * Run the handler chain in a loop, to let it process the whole input batch.
     * Once all the data is processed, null event will be returned and this
     * do-while loop will end.
     * @formatter:on
     */
    private Status execute0() throws HandlerException {
        Status status = Status.READY;
        int iteration = 0;
        String currentHandlerName = null;
        // HandlerContext.get().reset();
        Stack<HandlerNode> callbackHandlerIndexStack = new Stack<>();
        callbackHandlerIndexStack.push(handlerNodeHead);
        while (!callbackHandlerIndexStack.isEmpty()) {
            try {
                handlerChainExecutionCount++;
                logger.debug("handler chain executing",
                        "handlers=\"{}\" state=\"{}\" handlerChainExecutionCount=\"{}\"", handlers, state,
                        handlerChainExecutionCount);
                if ((state == STATE.STOPPING) || Thread.interrupted()) {
                    logger.alert(ALERT_TYPE.INGESTION_STOPPED, ALERT_CAUSE.SHUTDOWN_COMMAND, ALERT_SEVERITY.NORMAL,
                            "command received to shutdown the handler chain execution");
                    break;
                }

                iteration++;
                HandlerNode tempHandlerNode = callbackHandlerIndexStack.pop();
                boolean backoff = false;
                do {
                    Handler handler = tempHandlerNode.getHandler();
                    logger.debug("handler chain executing",
                            "_message=\"picked up next handler\" handler_chain=\"{}\" handler_id=\"{}\" handler_name=\"{}\" total_iterations=\"{}\"",
                            handlerChainName, handler.getId(), handler.getName(), iteration);
                    currentHandlerName = handler.getName();
                    status = handler.process();
                    // TODO: handle the null status
                    switch (status) {
                        case BACKOFF:
                        case BACKOFF_NOW:
                            logger.debug("handler chain executing",
                                    "_message=\"handler will backoff\" handler_chain=\"{}\" handler_id=\"{}\" handler_name=\"{}\" total_iterations=\"{}\" status=\"\"",
                                    handlerChainName, handler.getId(), handler.getName(), iteration, status);
                            backoff = true;
                            break;
                        // return status;
                        case CALLBACK:
                            logger.debug("handler chain executing",
                                    "_message=\"pushing handler node stack\" handler_chain=\"{}\" handler_id=\"{}\" handler_name=\"{}\" total_iterations=\"{}\"",
                                    handlerChainName, handler.getId(), handler.getName(), iteration);
                            callbackHandlerIndexStack.push(tempHandlerNode);
                            break;
                        case READY:
                            logger.debug("handler chain executing",
                                    "_message=\"handler ready\" handler_chain=\"{}\" handler_id=\"{}\" handler_name=\"{}\" total_iterations=\"{}\"",
                                    handlerChainName, handler.getId(), handler.getName(), iteration);
                            break;
                        default:
                            // do nothing
                    }
                    tempHandlerNode = tempHandlerNode.getNext();
                } while (tempHandlerNode != null && !backoff);
            } catch (Exception ex) {
                errorCount++;
                status = Status.CALLBACK;
                logger.alert(ALERT_TYPE.INGESTION_STOPPED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                        ALERT_SEVERITY.BLOCKER, ex,
                        "_message=\"one of the handlers({}) in the chain threw an unknown exception, this loop of handlers will be stopped after {} errors\" exception=\"{}\" error_count=\"{}\" handlerChainExecutionCount=\"{}\"",
                        currentHandlerName, thresholdErrorCount, ex.getMessage(), errorCount,
                        handlerChainExecutionCount);
                logger.info("handler chain handling exception", "invoking handleException");
                executeHandleException();
                if (errorCount > thresholdErrorCount) {
                    stopOnError();
                    if (ex instanceof HandlerException)
                        throw ex;
                    else
                        throw new HandlerException(ex.getMessage(), ex);
                }
            }
        }
        // while (true);
        return status;
    }

    private void stopOnError() throws IllegalHandlerStateException {
        logger.warn("handler chain in error",
                "_message=\"stopping the handler chain due to an error\" handler_chain=\"{}\" state=\"{}\"",
                handlerChainName, state);
        updateState(STATE.STOPPING);
        updateState(STATE.STOPPED);
    }

    private void executeHandleException() {
        Stack<HandlerNode> callbackHandlerIndexStack = new Stack<>();
        callbackHandlerIndexStack.push(handlerNodeHead);
        if (handlerNodeHead == null)
            return;
        HandlerNode tempHandlerNode = handlerNodeHead;
        do {
            Handler handler = tempHandlerNode.getHandler();
            logger.debug("handler chain handling exception",
                    "_message=\"picked up next handler\" handler_chain=\"{}\" handler_id=\"{}\" handler_name=\"{}\"",
                    handlerChainName, handler.getId(), handler.getName());
            tempHandlerNode.getHandler().handleException();
            tempHandlerNode = tempHandlerNode.getNext();
        } while (tempHandlerNode != null);
    }

    /**
     * @throws IllegalHandlerStateException
     * @formatter:on Execute steps to shutdown the handler manager.
     */
    public void shutdown() throws IllegalHandlerStateException {
        logger.info("handler chain shutting down",
                "_message=\"received a command to shutdown the handler chain\" handler_chain=\"{}\" state=\"{}\"",
                handlerChainName, state);
        updateState(STATE.STOPPING);
    }

    private boolean updateState(STATE state) throws IllegalHandlerStateException {
        if (state == STATE.RUNNING) {
            return updateStateToRunning();
        } else if (state == STATE.STOPPING) {
            return updateStateToStopping();
        } else if (state == STATE.STOPPED) {
            return updateStateToStopped();
        }

        return false;
    }

    private boolean updateStateToRunning() throws IllegalHandlerStateException {
        if (((state == STATE.INITIALIZED)) || (state == STATE.STOPPED)) {
            state = STATE.RUNNING;
            logger.debug("handler chain will execute, updating state",
                    "handler_chain=\"{}\" current_state=\"{}\" new_state=\"{}\"", handlerChainName, state,
                    STATE.RUNNING);
            return true;
        } else {
            logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
                    "_message=\"handler chain start failed\" handler_chain=\"{}\" current_state=\"{}\" new_state=\"{}\"",
                    handlerChainName, state, STATE.RUNNING);
            throw new IllegalHandlerStateException("handler can't come to running state from " + state);
        }
    }

    private boolean updateStateToStopping() {
        if (state == STATE.RUNNING) {
            logger.debug("handler chain stopping", "handler_chain=\"{}\" current_state=\"{}\" new_state=\"{}\"",
                    handlerChainName, state, STATE.STOPPING);
            state = STATE.STOPPING;
            return true;
        } else if (state == STATE.STOPPED) {
            logger.warn("handler chain stopping, no action taken",
                    "handler_chain=\"{}\" current_state=\"{}\" new_state=\"{}\"", handlerChainName, state,
                    STATE.STOPPING);
            return false;
        } else {
            logger.warn("handler chain stopping, no action taken",
                    "handler_chain=\"{}\" current_state=\"{}\" new_state=\"{}\"", handlerChainName, state,
                    STATE.STOPPING);
            return false;
        }
    }

    private boolean updateStateToStopped() {
        logger.debug("handler chain executed", "handler_chain=\"{}\" current_state=\"{}\" new_state=\"{}\"",
                handlerChainName, state, STATE.STOPPED);
        if (state != STATE.STOPPED) {
            state = STATE.STOPPED;
            return true;
        }
        return false;
    }
}
