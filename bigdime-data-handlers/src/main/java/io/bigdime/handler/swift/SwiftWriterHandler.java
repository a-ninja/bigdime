package io.bigdime.handler.swift;

import java.util.List;
import java.util.regex.Pattern;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.handler.AbstractHandler;

/**
 * 
 * @author Neeraj Jain
 *
 */

@Component
@Scope("prototype")
public abstract class SwiftWriterHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(SwiftWriterHandler.class));
	private String handlerPhase = "building SwiftWriterHandler";

	protected String username;
	protected String password; // make it char[]
	protected String authUrl;
	protected String tenantId;
	protected String tenantName;

	protected String containerName;

	protected AccountConfig config;
	protected Account account;
	protected Container container;
	protected String inputFilePathPattern;
	protected String outputFilePathPattern;
	protected Pattern inputPattern;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building SwiftWriterHandler";
		logger.info(handlerPhase, "properties={}", getPropertyMap());

		username = PropertyHelper.getStringProperty(getPropertyMap(), SwiftWriterHandlerConstants.USER_NAME);
		password = PropertyHelper.getStringProperty(getPropertyMap(), SwiftWriterHandlerConstants.PASSWORD);
		authUrl = PropertyHelper.getStringProperty(getPropertyMap(), SwiftWriterHandlerConstants.AUTH_URL);
		tenantId = PropertyHelper.getStringProperty(getPropertyMap(), SwiftWriterHandlerConstants.TENANT_ID);
		tenantName = PropertyHelper.getStringProperty(getPropertyMap(), SwiftWriterHandlerConstants.TENANT_NAME);
		containerName = PropertyHelper.getStringProperty(getPropertyMap(), SwiftWriterHandlerConstants.CONTAINER_NAME);

		inputFilePathPattern = PropertyHelper.getStringProperty(getPropertyMap(),
				SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN);
		outputFilePathPattern = PropertyHelper.getStringProperty(getPropertyMap(),
				SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN);

		final String objType = PropertyHelper.getStringProperty(getPropertyMap(),
				SwiftWriterHandlerConstants.UPLOAD_OBJECT_TYPE);

		logger.debug(handlerPhase,
				"username={} authUrl={} tenantId={} tenantName={} containerName={} inputFilePathPattern=\"{}\" outputFilePathPattern=\"{}\"",
				username, authUrl, tenantId, tenantName, containerName, inputFilePathPattern,
				outputFilePathPattern);
		config = new AccountConfig();
		config.setUsername(username);
		config.setPassword(password);
		config.setAuthUrl(authUrl);
		config.setTenantId(tenantId);
		config.setTenantName(tenantName);
		account = new AccountFactory(config).createAccount();
		container = account.getContainer(containerName);

		String pattern = "\\/\\w*\\/\\w*\\/\\w*\\/\\w*\\/\\w*\\/(\\w*)\\/(\\w*)\\/(\\w*)";
		logger.debug(handlerPhase, "_message=\"created account\"");

		inputPattern = Pattern.compile(pattern);
	}

	/**
	 * @formatter:off
	 * Input ActionEvent contains following headers:
	 * 1. entityName
	 * 2. objectName
	 * 3. ActionEventHeaderConstants.READ_COMPLETE
	 * Body contains the actual byte buffer to be written to
	 * 
	 * 
	 * In Swift:
	 * n-data/2016-07-05/lstg_item_cndtn/000188_0.gz
	 * 
	 * container name = new-data
	 * container name = 2016-07-05
	 * object name = item
	 * 
	 * @formatter:on
	 * 
	 */
	@Override
	public Status process() throws HandlerException {

		handlerPhase = "processing SwiftWriterHandler";
		incrementInvocationCount();

		SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);

		if (journal == null || journal.getEventList() == null) {
			// process for ready status.
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
			logger.info(handlerPhase, "journal is null, actionEvents.size={} id={} ", actionEvents.size(), getId());
			if (actionEvents.isEmpty())
				return Status.BACKOFF;

			return process0(actionEvents);

		} else {
			List<ActionEvent> actionEvents = journal.getEventList();

			logger.info(handlerPhase, "journal is not null, actionEvents==null={}", (actionEvents == null));
			if (actionEvents != null && !actionEvents.isEmpty()) {
				// process for CALLBACK status.
				return process0(journal.getEventList());
			}
		}
		return null;
	}
	protected abstract Status process0(List<ActionEvent> actionEvents) throws HandlerException;
//	private Status process0(List<ActionEvent> actionEvents) throws HandlerException {
//
//		SwiftWriterHandlerJournal journal = getJournal(SwiftWriterHandlerJournal.class);
//		if (journal == null) {
//			logger.debug(handlerPhase, "jounral is null, initializing");
//			journal = new SwiftWriterHandlerJournal();
//			getHandlerContext().setJournal(getId(), journal);
//		}
//		Status statusToReturn = Status.READY;
//		long startTime = System.currentTimeMillis();
//		// ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		try {
//
//			// String swiftObjectName = null;
//
//			boolean writeReady = false;
//			int eventsToWrite = 0;
//			ActionEvent actionEventToWrite = null;
//			for (ActionEvent actionEvent : actionEvents) {
//				eventsToWrite++;
//				logger.debug(handlerPhase, "headers={}", actionEvent.getHeaders());
//
//				String readCompleteStr = actionEvent.getHeaders().get(ActionEventHeaderConstants.READ_COMPLETE);
//
//				if (!StringUtils.isBlank(readCompleteStr) && Boolean.parseBoolean(readCompleteStr) == true) {
//					writeReady = true;
//					actionEventToWrite = actionEvent;
//					break;
//				}
//			}
//
//			if (writeReady) {
//				writeToSwift(actionEventToWrite, actionEvents, eventsToWrite);
//			}
//			if (!actionEvents.isEmpty()) {
//				journal.setEventList(actionEvents);
//			}
//			journal.setEventList(actionEvents);
//		} catch (Exception e) {
//			throw new HandlerException(e.getMessage(), e);
//		}
//		long endTime = System.currentTimeMillis();
//		logger.debug(handlerPhase, "statusToReturn={}", statusToReturn);
//		logger.info(handlerPhase, "SwiftWriter finished in {} milliseconds", (endTime - startTime));
//		return statusToReturn;
//
//	}

//	private void writeToSwift(final ActionEvent actionEventToWrite, List<ActionEvent> actionEvents,
//			final int eventsToWrite) throws IOException, HandlerException {
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		try {
//			String fileName = actionEventToWrite.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
//			String swiftObjectName = outputFilePathPattern;
//			Matcher m = inputPattern.matcher(fileName);
//
//			while (m.find()) {
//				logger.debug(handlerPhase, "_message=\"matched filename\" filename={}", m.group());
//				String key = null;
//
//				for (int i = 1; i <= m.groupCount(); i++) {
//					key = "$" + i;
//					String temp = m.group(i);
//					logger.debug(handlerPhase, "file-part={}", temp);
//					swiftObjectName = swiftObjectName.replace(key, temp);
//					logger.debug(handlerPhase, "objectName={}", swiftObjectName);
//				}
//				logger.debug(handlerPhase, "final objectName={}", swiftObjectName);
//
//			}
//			for (int i = 0; i < eventsToWrite; i++) {
//				baos.write(actionEvents.remove(0).getBody());
//			}
//
//			if (uploadObjectType == SwiftUploadObjectType.BYTES) {
//				logger.debug(handlerPhase, "_message=\"writing to swift\" swiftObjectName={}", swiftObjectName);
//				uploadBytes(container, swiftObjectName, baos.toByteArray());
//			} else {
//				throw new HandlerException("SwiftWriterHandler does not support uploading files");
//			}
//		} finally {
//			try {
//				baos.close();
//			} catch (IOException e) {
//				logger.warn(handlerPhase, "exception while trying to close the ByteArrayOutputStream", e);
//				// duck
//			}
//		}
//	}

//	private void uploadFile(final Container container, final String objectName, final byte[] data) {
//		throw new UnsupportedOperationException();
//	}
//
//	private void uploadBytes(final Container container, final String objectName, final byte[] data) {
//		StoredObject object = container.getObject(objectName);
//		object.uploadObject(data);
//	}

}
