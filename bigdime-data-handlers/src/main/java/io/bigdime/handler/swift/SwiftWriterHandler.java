package io.bigdime.handler.swift;

import com.google.common.base.Preconditions;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.libs.client.SwiftClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Neeraj Jain
 */

@Component
@Scope("prototype")
public abstract class SwiftWriterHandler extends AbstractHandler {
  private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(SwiftWriterHandler.class));

  protected String containerName;

  protected String inputFilePathPattern;
  protected String outputFilePathPattern;
  protected Pattern inputPattern;

  private boolean initialized = false;
  @Autowired
  protected SwiftClient swiftClient;

  @Override
  public void build() throws AdaptorConfigurationException {
    super.build();
    setHandlerPhase("building SwiftWriterHandler");
    logger.info(getHandlerPhase(), "properties={}", getPropertyMap());


    inputFilePathPattern = PropertyHelper.getStringProperty(getPropertyMap(),
            SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN);
    outputFilePathPattern = PropertyHelper.getStringProperty(getPropertyMap(),
            SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN);

    inputPattern = Pattern.compile(inputFilePathPattern);
  }

  protected void init() {
    logger.info(getHandlerPhase(), "_message=\"connecting to swift\"");
    // String pattern =
    // "\\/\\w*\\/\\w*\\/\\w*\\/\\w*\\/\\w*\\/(\\w*)\\/(\\w*)\\/(\\w*)";
    logger.info(getHandlerPhase(), "_message=\"connected to swift, created account object\"");
  }

  /**
   * @formatter:off Input ActionEvent contains following headers:
   * 1. entityName
   * 2. objectName
   * 3. ActionEventHeaderConstants.READ_COMPLETE
   * Body contains the actual byte buffer to be written to
   * <p>
   * <p>
   * In Swift:
   * new-data/2016-07-05/items/file.gz
   * <p>
   * container name = new-data
   * container name = 2016-07-05
   * object name = item
   * @formatter:on
   */
  @Override
  public Status process() throws HandlerException {

    incrementInvocationCount();
    if (!initialized) {
      init();
      initialized = true;
    }

    SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
    Status returnStatus = Status.READY;
    if (journal == null || journal.getEventList() == null) {
      // process for ready status.
      List<ActionEvent> actionEvents = getHandlerContext().getEventList();
      Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
      logger.info(getHandlerPhase(), "journal is null, actionEvents.size={} id={} ", actionEvents.size(),
              getId());

      if (actionEvents.isEmpty())
        returnStatus = Status.BACKOFF_NOW;
      else
        returnStatus = process0(actionEvents);
    } else {
      List<ActionEvent> actionEvents = journal.getEventList();

      logger.info(getHandlerPhase(), "journal not null, actionEvents==null={}", (actionEvents == null));
      if (actionEvents != null && !actionEvents.isEmpty()) {
        // process for CALLBACK status.
        logger.info(getHandlerPhase(), "journal not null or empty, actionEvents.size={}", actionEvents.size());
        List<ActionEvent> moreEvents = getHandlerContext().getEventList();

				/*
                 * TODO: Dangerous code block, try to simplify it.
				 */
        if (moreEvents != null)
          journal.getEventList().addAll(moreEvents);
        returnStatus = process0(journal.getEventList());
      } else {
        returnStatus = Status.BACKOFF_NOW;
      }
    }
    processLastHandler();
    return returnStatus;
  }

  protected abstract Status process0(List<ActionEvent> actionEvents) throws HandlerException;
}
