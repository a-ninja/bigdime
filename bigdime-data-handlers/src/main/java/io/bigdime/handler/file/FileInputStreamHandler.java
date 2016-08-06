/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.FileHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

/**
 * A handler that reads data from a file. The data is read in binary or text
 * depending on the handler configuration.
 * @formatter:off
 * Read a file from given location.
 * Read the location of the file.
 * Read the buffer size.
 * if context is null
 * 	start_index=0;
 * else
 * 	start_index=bytes_read_already
 *
 *
 * File:
 *
 * Use multiple channels if:
 * reader is faster
 * record boundary
 * order of records doesn't matter.
 *
 * Use single outputChannel if any of the below is true:
 * checksum is required
 * order or the records need to be maintained
 * @formatter:on
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class FileInputStreamHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(FileInputStreamHandler.class));
	public static final String FILE_LOCATION = "fileLocation";

	private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;

	private RandomAccessFile file;

	private File currentFile;

	private String fileNamePattern;

	private int bufferSize;

	/**
	 * Base path, absolute, to look for the files.
	 */
	private String fileLocation;

	private long fileLength = -1;

	private FileChannel fileChannel;

	private FileInputDescriptor fileInputDescriptor;

	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;
	@Autowired
	private FileHelper fileHelper;

	private String handlerPhase = "";
	private String entityName;

	private String basePath = "/";
	private boolean preserveBasePath;
	private boolean preserveRelativePath;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building FileInputStreamHandler";
		logger.info(handlerPhase, "properties={}", getPropertyMap());

		@SuppressWarnings("unchecked")
		Entry<String, String> srcDescInputs = (Entry<String, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescInputs == null) {
			throw new InvalidValueConfigurationException("src-desc can't be null");
		}
		logger.debug(handlerPhase, "entity:fileNamePattern={} input_field_name={}", srcDescInputs.getKey(),
				srcDescInputs.getValue());

		basePath = PropertyHelper.getStringProperty(getPropertyMap(), FileInputStreamReaderHandlerConstants.BASE_PATH,
				"/");
		logger.debug(handlerPhase, "basePath={}", basePath);

		fileInputDescriptor = new FileInputDescriptor();
		try {
			fileInputDescriptor.parseDescriptor(basePath);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"src-desc must contain entityName:fileNamePrefix, e.g. user:web_user*");
		}

		fileLocation = fileInputDescriptor.getPath(); // srcDescInputs.getKey();
		if (fileLocation == null) {
			throw new InvalidValueConfigurationException("filePath in src-desc can't be null");
		}

		String[] srcDesc = fileInputDescriptor.parseSourceDescriptor(srcDescInputs.getKey());

		entityName = srcDesc[0];
		fileNamePattern = srcDesc[1];
		logger.debug(handlerPhase, "entityName={} fileNamePattern={}", entityName, fileNamePattern);

		bufferSize = PropertyHelper.getIntProperty(getPropertyMap(), FileInputStreamReaderHandlerConstants.BUFFER_SIZE,
				DEFAULT_BUFFER_SIZE);
		logger.debug(handlerPhase, "id={} fileLocation={}", getId(), fileLocation);
		preserveBasePath = PropertyHelper.getBooleanProperty(getPropertyMap(),
				FileInputStreamReaderHandlerConstants.PRESERVE_BASE_PATH);
		preserveRelativePath = PropertyHelper.getBooleanProperty(getPropertyMap(),
				FileInputStreamReaderHandlerConstants.PRESERVE_RELATIVE_PATH);

		logger.debug(handlerPhase, "id={}", getId());
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing FileInputStreamHandler";
		incrementInvocationCount();
		try {
			Status status = preProcess();
			if (status == Status.BACKOFF) {
				logger.debug(handlerPhase, "returning BACKOFF");
				return status;
			}
			return doProcess();
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"error during reading file", e);
			throw new HandlerException("Unable to process message from file", e);
		} catch (RuntimeInfoStoreException e) {
			throw new HandlerException("Unable to process message from file", e);
		}
	}

	/*
	 * @formatter:off
	 * if (firstRun) {
	 * 	get runtime_info with status=start
	 * 	if (found more than one) {
	 * 		foundDirty=true
	 * 		dirtyRecordCount=n
	 * 		log
	 * 	}
	 * }
	 * 
	 * preprocess() {
	 * 	getRecordWithState = START
	 * 	if found() {
	 * 		header.recordDirty=true
	 * 	}
	 * } 
	 * 
	 * @formatter:on
	 * 
	 */
	long dirtyRecordCount = 0;
	List<RuntimeInfo> dirtyRecords;
	private boolean processingDirty = false;

	@Override
	protected Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException {
		if (isFirstRun()) {
			dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, entityName);
			if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
				dirtyRecordCount = dirtyRecords.size();
				logger.warn(handlerPhase,
						"_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entityName={}",
						getId(), dirtyRecordCount, entityName);
			} else {
				logger.info(handlerPhase, "_message=\"no dirty records found\" handler_id={}", getId());
			}
		}
		if (readAllFromFile()) {
			setNextFileToProcess();
			if (file == null) {
				logger.info(handlerPhase, "_message=\"no file to process\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			fileLength = file.length();
			logger.info(handlerPhase, "_message=\"got a new file to process\" handler_id={} file_length={}", getId(),
					fileLength);
			if (fileLength == 0) {
				logger.info(handlerPhase, "_message=\"file is empty\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			getSimpleJournal().setTotalSize(fileLength);
			fileChannel = file.getChannel();
			return Status.READY;
		}
		return Status.READY;
	}

	@Override
	protected Status doProcess() throws IOException, HandlerException, RuntimeInfoStoreException {

		long nextIndexToRead = getTotalReadFromJournal();
		logger.debug(handlerPhase, "handler_id={} next_index_to_read={} buffer_size={}", getId(), nextIndexToRead,
				bufferSize);
		fileChannel.position(nextIndexToRead);
		final ByteBuffer readInto = ByteBuffer.allocate(bufferSize);

		int bytesRead = fileChannel.read(readInto, nextIndexToRead);
		logger.debug(handlerPhase, "handler_id={} bytes_read={}", getId(), bytesRead);

		if (bytesRead > 0) {
			getSimpleJournal().setTotalRead((nextIndexToRead + bytesRead));
			ActionEvent outputEvent = new ActionEvent();
			byte[] readBody = new byte[bytesRead];
			logger.debug(handlerPhase, "handler_id={} readBody.length={} remaining={}", getId(), readBody.length,
					readInto.remaining());
			readInto.flip();
			readInto.get(readBody, 0, bytesRead);
			outputEvent.setBody(readBody);
			// outputEvent.getHeaders().put(ActionEventHeaderConstants.ABSOLUTE_FILE_PATH,
			// currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME, currentFile.getName());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_SIZE,
					String.valueOf(getTotalSizeFromJournal()));
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_TOTAL_READ,
					String.valueOf(getTotalReadFromJournal()));
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_LOCATION, currentFile.getParent());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.BASE_PATH, basePath);
			String relativeToBasePath = "";
			if (currentFile.getParent().length() > basePath.length())
				relativeToBasePath = currentFile.getParent().substring(basePath.length());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.RELATIVE_PATH, relativeToBasePath);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.PRESERVE_BASE_PATH,
					String.valueOf(preserveBasePath));
			outputEvent.getHeaders().put(ActionEventHeaderConstants.PRESERVE_RELATIVE_PATH,
					String.valueOf(preserveRelativePath));
			logger.debug(handlerPhase, "setting entityName header, value={}", entityName);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);
			logger.debug(handlerPhase, "setting CLEANUP_REQUIRED header, processingDirty={} ", processingDirty);
			if (processingDirty)
				outputEvent.getHeaders().put(ActionEventHeaderConstants.CLEANUP_REQUIRED, "true");
			processingDirty = false;// CLEANUP_REQUIRED needs to be done only
									// for the first time the file being read
			outputEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.FALSE.toString());

			getHandlerContext().createSingleItemEventList(outputEvent);

			logger.debug(handlerPhase,
					"_message=\"handler read data, ready to return\", context.list_size={} total_read={} total_size={} context={}",
					getHandlerContext().getEventList().size(), getTotalReadFromJournal(), getTotalSizeFromJournal(),
					getHandlerContext());

			processChannelSubmission(outputEvent);

			if (readAllFromFile()) {
				getSimpleJournal().reset();
				logger.debug(handlerPhase,
						"_message=\"done reading file={}, there might be more files to process, returning CALLBACK\" handler_id={} headers_from_file_handler={}",
						currentFile.getAbsolutePath(), getId(), outputEvent.getHeaders());
				outputEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
				return Status.CALLBACK;
			} else {
				logger.debug(handlerPhase, "\"there is more data to process, returning CALLBACK\" handler_id={}",
						getId());
				return Status.CALLBACK;
			}
		} else {
			logger.debug(handlerPhase, "returning READY, no data read from the file");
			return Status.READY;
		}
	}

	protected void setNextFileToProcess() throws IOException, RuntimeInfoStoreException, HandlerException {

		if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
			RuntimeInfo dirtyRecord = dirtyRecords.remove(0);
			logger.info(handlerPhase, "\"processing a dirty record\" dirtyRecord=\"{}\"", dirtyRecord);
			String nextDescriptorToProcess = dirtyRecord.getInputDescriptor();
			initFile(nextDescriptorToProcess);
			processingDirty = true;
			return;
		} else {
			logger.info(handlerPhase, "processing a clean record");
			processingDirty = false;
			RuntimeInfo queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName);
			if (queuedRecord == null) {
				boolean foundRecordsOnDisk = findAndAddRuntimeInfoRecords();
				if (foundRecordsOnDisk)
					queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName);
			}
			if (queuedRecord != null) {
				initFile(queuedRecord.getInputDescriptor());
				updateRuntimeInfo(runtimeInfoStore, entityName, queuedRecord.getInputDescriptor(),
						RuntimeInfoStore.Status.STARTED);
			} else {
				file = null;
			}
		}
	}

	protected boolean findAndAddRuntimeInfoRecords() throws RuntimeInfoStoreException {
		List<String> availableFiles = getAvailableFiles();
		if (availableFiles == null || availableFiles.isEmpty()) {
			return false;
		}
		for (String file : availableFiles) {
			queueRuntimeInfo(runtimeInfoStore, entityName, file);
		}
		return true;
	}

	private void initFile(String nextDescriptorToProcess) throws IOException {
		if (file != null) {
			file.close();
		}
		if (fileChannel != null) { // closing the channel explicitly
			fileChannel.close();
		}
		file = new RandomAccessFile(nextDescriptorToProcess, "r");
		currentFile = new File(nextDescriptorToProcess);
		logger.debug(handlerPhase, "absolute_path={} file_name_for_descriptor={}", currentFile.getAbsolutePath(),
				currentFile.getName());
	}

	protected List<String> getAvailableFiles() {
		try {
			logger.debug(handlerPhase, "getting next file list, fileLocation=\"{}\" fileNamePattern=\"{}\"",
					fileLocation, fileNamePattern);
			return fileHelper.getAvailableFiles(fileLocation, fileNamePattern);
		} catch (IllegalArgumentException ex) {
			logger.warn(handlerPhase,
					"_message=\"no file found, make sure fileLocation and fileNamePattern are correct\" fileLocation={} fileNamePattern={}",
					fileLocation, fileNamePattern, ex);
		}
		return null;
	}

	@Override
	public void shutdown() {
		super.shutdown();
		shutdown0();
	}

	private void shutdown0() {
		throw new NotImplementedException();
	}

	private long getTotalReadFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalSize();
	}

	private SimpleJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}

	private boolean readAllFromFile() throws HandlerException {
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}

	protected String getHandlerPhase() {
		return handlerPhase;
	}

	public String getFileNamePattern() {
		return fileNamePattern;
	}

	public String getEntityName() {
		return entityName;
	}

	public String getBasePath() {
		return basePath;
	}

}
