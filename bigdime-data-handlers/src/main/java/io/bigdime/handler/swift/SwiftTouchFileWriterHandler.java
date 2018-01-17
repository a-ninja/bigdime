package io.bigdime.handler.swift;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.javaswift.joss.model.DirectoryOrObject;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;

@Component
@Scope("prototype")
public final class SwiftTouchFileWriterHandler extends AbstractByteWriterHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(SwiftTouchFileWriterHandler.class));

	protected String filePathPrefixPattern;// Like directory
	protected String filePathPattern;// like, directoryName+fileName

	@Override
	public void build() throws AdaptorConfigurationException {
		// If the objectName = 20160101__part1_item_part3_part4/000304_0.ext,
		// prefix is 20160101__part1_item_part3_part4
		super.build();
		setHandlerPhase("building SwiftTouchFileWriterHandler");
		filePathPrefixPattern = PropertyHelper.getStringProperty(getPropertyMap(),
				SwiftWriterHandlerConstants.FILE_PATH_PREFIX_PATTERN);
		filePathPattern = PropertyHelper.getStringProperty(getPropertyMap(),
				SwiftWriterHandlerConstants.FILE_PATH_PATTERN);

		logger.debug(getHandlerPhase(), "	={} outputFilePathPattern={} filePathPrefixPattern={} filePathPattern={}",
				outputFilePathPattern, inputFilePathPattern, filePathPrefixPattern, filePathPattern);
	}

	@Override
	public Status process() throws HandlerException {
		setHandlerPhase("processing SwiftTouchFileWriterHandler");
		return super.process();
	}

	@Override
	protected Status process0(List<ActionEvent> actionEvents) throws HandlerException {

		SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
		if (journal == null) {
			logger.debug(getHandlerPhase(), "jounral is null, initializing");
			journal = new SwiftWriterHandlerJournal();
			getHandlerContext().setJournal(getId(), journal);
		}
		Status statusToReturn = Status.READY;
		long startTime = System.currentTimeMillis();
		try {

			boolean writeReady = false;
			final int numEventsToWrite = 1;
			// for (ActionEvent actionEvent : actionEvents) {
			ActionEvent actionEvent = actionEvents.get(0);
			// ActionEvent actionEventToWrite = actionEvent;

			// numEventsToWrite++;

			byte[] body = actionEvent.getBody();

			String[] fileNames = new String(body, Charset.defaultCharset()).split("\n");
			body = null;
			logger.info(getHandlerPhase(), "sourceFileNames.count={}", fileNames.length);
			String sourceFileName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);

			// Get the source file name =
			// /webhdfs/v1/user/johndow/bigdime/newdir3/20160101/part1_prt2_part3/0001_0.ext
			String swiftPrefix = StringHelper.replaceTokens(sourceFileName, filePathPrefixPattern, inputPattern,
					actionEvent.getHeaders());

			Collection<DirectoryOrObject> swiftDirListing = swiftClient.container().listDirectory(swiftPrefix, null, null, -1);
			logger.debug(getHandlerPhase(), "_message=\"swiftDirListing\" swiftDirListing_null={} swiftPrefix={}",
					(swiftDirListing == null), swiftPrefix);
			if (swiftDirListing != null) {
				logger.info(getHandlerPhase(), "_message=\"swiftDirListing\" swiftDirListing.count={} swiftPrefix={}",
						swiftDirListing.size(), swiftPrefix);
			}

			int matchedCount = 0;
			final Set<String> fileNameListFromSwift = new HashSet<>();
			final Set<String> fileBareNameListFromSwift = new LinkedHashSet<>();
			if (swiftDirListing != null && (swiftDirListing.size() >= fileNames.length)) {
				logger.info(getHandlerPhase(), "_message=\"file count match\" count={}", swiftDirListing.size());

				for (DirectoryOrObject dirOrObject : swiftDirListing) {
					fileNameListFromSwift.add(dirOrObject.getName());
					fileBareNameListFromSwift.add(dirOrObject.getAsObject().getBareName());
				}

				for (String webhdfsFileName : fileNames) {
					String swiftFileName = StringHelper.replaceTokens(webhdfsFileName, filePathPattern, inputPattern,
							actionEvent.getHeaders());
					if (fileNameListFromSwift.contains(swiftFileName)) {
						logger.debug(getHandlerPhase(), "_message=\"file name found in swift\" swiftFileName={}",
								swiftFileName);
						matchedCount++;
					} else {
						logger.debug(getHandlerPhase(), "_message=\"file not found in swift\" swiftFileName={}",
								swiftFileName);
						break;
					}
				}

			}

			if (matchedCount == fileNames.length) {
				logger.info(getHandlerPhase(), "_message=\"setting writeReady to true\"");
				// logger.debug(getHandlerPhase(), "_message=\"bare names={}",
				// fileBareNameListFromSwift);
				StringBuilder outputEventBody = new StringBuilder();
				for (final String swiftFileFragmentName : fileBareNameListFromSwift) {
					outputEventBody.append(swiftFileFragmentName).append("\n");
				}
				actionEvent.setBody(outputEventBody.toString().getBytes());
				writeReady = true;
			} else {
				logger.debug(getHandlerPhase(),
						"_message=\"setting writeReady to false\" matchedCount={} fileNames.length={}", matchedCount,
						fileNames.length);
			}

			// If writeReady, write to swift and clear those events from list.
			if (writeReady) {
				logger.debug(getHandlerPhase(), "_message=\"calling writeToSwift\" numEventsToWrite={}",
						numEventsToWrite);
				List<ActionEvent> eventListToWrite = actionEvents.subList(0, numEventsToWrite);
				ActionEvent outputEvent = writeToSwift(actionEvent, eventListToWrite);

				getHandlerContext().createSingleItemEventList(outputEvent);
				journal.reset();
				eventListToWrite.clear();
			}

			// if we wrote and there is more data to write, callback
			if (writeReady) {
				if (!actionEvents.isEmpty()) {
					logger.debug(getHandlerPhase(), "_message=\"returning callback\" actionEvents.size={}",
							actionEvents.size());
					journal.setEventList(actionEvents);
					statusToReturn = Status.CALLBACK;
				} else {
					logger.debug(getHandlerPhase(), "_message=\"returning ready\" actionEvents.size={}",
							actionEvents.size());
					statusToReturn = Status.READY;
				}
			} else {
				statusToReturn = Status.BACKOFF_NOW; // no need to call next
														// handler
			}
		} catch (Exception e) {
			throw new HandlerException(e.getMessage(), e);
		}
		long endTime = System.currentTimeMillis();
		logger.debug(getHandlerPhase(), "statusToReturn={}", statusToReturn);
		logger.info(getHandlerPhase(), "SwiftTouchFileWriterHandler finished in {} milliseconds",
				(endTime - startTime));
		return statusToReturn;

	}
}
