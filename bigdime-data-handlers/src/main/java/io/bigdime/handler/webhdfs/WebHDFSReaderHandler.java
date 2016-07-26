package io.bigdime.handler.webhdfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.file.FileInputStreamReaderHandlerConstants;
import io.bigdime.libs.hdfs.FileStatus;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsFactory;
import io.bigdime.libs.hdfs.WebHdfsReader;

/**
 * @formatter:off
 * Read the directory name from the headers.
 * Read the directory listing.
 *    
 * The fileLocation is going to contain multiple files.
 *    
 * 
 * 
 * @formatter:on
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class WebHDFSReaderHandler extends AbstractSourceHandler {

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(WebHDFSWriterHandler.class));
	private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	private String hostNames;
	private int port;
	private String hdfsFileName;
	private String hdfsPath;
	private String hdfsUser;
	private WebHdfs webHdfs;
	private String entityName;
	private HDFS_AUTH_OPTION authOption;
	private int bufferSize;

	private String readHdfsPathFrom; // CONFIG | HEADERS

	private FileStatus currentFileStatus;
	private String currentFilePath;
	private long fileLength = -1;
	ReadableByteChannel fileChannel;

	private InputStream inputStream;
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	private long dirtyRecordCount = 0;
	protected List<RuntimeInfo> dirtyRecords;
	private boolean processingDirty = false;
	private InputDescriptor<String> inputDescriptor;

	private final String INPUT_DESCRIPTOR_PREFIX = "/webhdfs/v1/";

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		try {
			setHandlerPhase("building WebHDFSReaderHandler");
			logger.info(getHandlerPhase(), "building WebHDFSReaderHandler");
			hostNames = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HOST_NAMES);
			port = PropertyHelper.getIntProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.PORT);

			hdfsUser = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HDFS_USER);
			readHdfsPathFrom = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.READ_HDFS_PATH_FROM);
			if (StringUtils.equalsIgnoreCase(readHdfsPathFrom, "config")) {
				hdfsPath = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HDFS_PATH);
			}
			bufferSize = PropertyHelper.getIntProperty(getPropertyMap(),
					FileInputStreamReaderHandlerConstants.BUFFER_SIZE, DEFAULT_BUFFER_SIZE);

			final String authChoice = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString());

			authOption = HDFS_AUTH_OPTION.getByName(authChoice);
			logger.info(getHandlerPhase(),
					"hostNames={} port={} hdfsUser={} hdfsPath={} hdfsFileName={} readHdfsPathFrom={}  authChoice={} authOption={}",
					hostNames, port, hdfsUser, hdfsPath, hdfsFileName, readHdfsPathFrom, authChoice, authOption);
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}

	/**
	 * descriptor: entityName, webhdfsFilePath
	 */

	/**
	 * @formatter:off
	 * 
	 * if (first_time)
	 * 	getAvailableDirectoriesFromHeader
	 *  getFilesForEachDirectory
	 *  addFilesToRuntime
	 * Read all the events from event.
	 * For each event, read the hdfsPath and update RTI.
	 * Pick one file from RTI to process.
	 * 
	 * @formatter:on
	 */

	@Override
	protected Status doProcess() throws IOException, HandlerException, RuntimeInfoStoreException {
		long nextIndexToRead = getTotalReadFromJournal();
		logger.debug(getHandlerPhase(),
				"handler_id={} next_index_to_read={} buffer_size={} is_channel_open={} current_file_path={}", getId(),
				nextIndexToRead, bufferSize, fileChannel.isOpen(), currentFilePath);
		// fileChannel.position(nextIndexToRead);
		final ByteBuffer readInto = ByteBuffer.allocate(bufferSize);
		Status statustoReturn = Status.READY;

		int bytesRead = fileChannel.read(readInto);
		logger.debug(getHandlerPhase(), "handler_id={} bytes_read={}", getId(), bytesRead);
		if (bytesRead > 0) {
			getSimpleJournal().setTotalRead((nextIndexToRead + bytesRead));
			long readCount = getSimpleJournal().getReadCount();
			getSimpleJournal().setReadCount(readCount + 1);
			ActionEvent outputEvent = new ActionEvent();
			byte[] readBody = new byte[bytesRead];
			logger.debug(getHandlerPhase(), "handler_id={} readBody.length={} fileLength={} readCount={}", getId(),
					readBody.length, fileLength, getSimpleJournal().getReadCount());

			readInto.flip();
			readInto.get(readBody, 0, bytesRead);

			outputEvent.setBody(readBody);
			statustoReturn = Status.CALLBACK;
			if (readAll()) {
				logger.info(getHandlerPhase(), "\"read all data\" handler_id={} readCount={} current_file_path={}",
						getId(), getSimpleJournal().getReadCount(), currentFilePath);
				getSimpleJournal().reset();
				outputEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
				// ranOnce = true;
			} else {
				logger.debug(getHandlerPhase(), "\"there is more data to process, returning CALLBACK\" handler_id={}",
						getId());
			}
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME, currentFilePath);
			outputEvent.getHeaders().put("read_count", "" + getSimpleJournal().getReadCount());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, currentFilePath);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);

			logger.debug(getHandlerPhase(), "_message=\"checking process submission, headers={}\" handler_id={}",
					outputEvent.getHeaders(), getId());
			processChannelSubmission(outputEvent);
			return statustoReturn;
		} else {
			logger.info(getHandlerPhase(), "returning READY, no data read from the file");
			return Status.READY;
		}
	}

	@Override
	protected Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException {
		if (isFirstRun()) {
			entityName = getEntityNameFromHeader();
			logger.info(getHandlerPhase(), "From header, entityName={} ", entityName);
			dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, entityName);

			final Iterator<RuntimeInfo> dirtyRecordIter = dirtyRecords.iterator();
			while (dirtyRecordIter.hasNext()) {
				final RuntimeInfo dirtyRecord = dirtyRecordIter.next();
				if (!dirtyRecord.getInputDescriptor().startsWith(INPUT_DESCRIPTOR_PREFIX)) {
					logger.info(getHandlerPhase(),
							"_message=\"removing from dirty record list\" handler_id={} input_descriptor={} startWith={}",
							getId(), dirtyRecord.getInputDescriptor(),
							dirtyRecord.getInputDescriptor().startsWith(INPUT_DESCRIPTOR_PREFIX));
					dirtyRecordIter.remove();
				}
			}

			if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
				dirtyRecordCount = dirtyRecords.size();
				logger.warn(getHandlerPhase(),
						"_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entityName={}",
						getId(), dirtyRecordCount, entityName);
			} else {
				logger.info(getHandlerPhase(), "_message=\"no dirty records found\" handler_id={}", getId());
			}
		}
		if (readAll()) {
			try {
				setNextDescriptorToProcess();
			} catch (WebHdfsException e) {
				throw new HandlerException(e);
			}
			if (currentFilePath == null) {
				logger.info(getHandlerPhase(), "_message=\"no file to process\" handler_id={} descriptor={}", getId());
				return Status.BACKOFF;
			}

			fileLength = currentFileStatus.getLength();
			logger.info(getHandlerPhase(), "_message=\"got a new file to process\" handler_id={} file_length={}",
					getId(), fileLength);
			if (fileLength == 0) {
				logger.info(getHandlerPhase(), "_message=\"file is empty\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			getSimpleJournal().setTotalSize(fileLength);
		}
		return Status.READY;
	}

	protected void setNextDescriptorToProcess()
			throws IOException, RuntimeInfoStoreException, HandlerException, WebHdfsException {

		if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
			RuntimeInfo dirtyRecord = dirtyRecords.remove(0);
			logger.info(getHandlerPhase(), "\"processing a dirty record\" dirtyRecord=\"{}\"", dirtyRecord);
			String nextDescriptorToProcess = dirtyRecord.getInputDescriptor();
			initRecordToProcess(nextDescriptorToProcess);
			processingDirty = true;
			return;
		} else {
			logger.info(getHandlerPhase(), "processing a clean record");
			processingDirty = false;
			RuntimeInfo queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName, INPUT_DESCRIPTOR_PREFIX);
			if (queuedRecord == null) {
				boolean foundRecordsToProcess = initializeRuntimeInfoRecords();
				if (foundRecordsToProcess)
					queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName, INPUT_DESCRIPTOR_PREFIX);
			}
			if (queuedRecord != null) {
				logger.info(getHandlerPhase(), "_message=\"found a queued record, will process this\" queued_record={}",
						queuedRecord);
				initRecordToProcess(queuedRecord.getInputDescriptor());
				Map<String, String> properties = new HashMap<>();
				properties.put("handlerName", this.getClass().getName());
				updateRuntimeInfo(runtimeInfoStore, entityName, queuedRecord.getInputDescriptor(),
						RuntimeInfoStore.Status.STARTED, properties);
			} else {
				inputDescriptor = null;
			}
		}
	}

	private boolean initializeRuntimeInfoRecords() throws RuntimeInfoStoreException, IOException, WebHdfsException {
		WebHdfs webHdfs1 = null;
		boolean recordsFound = false;
		try {
			List<String> availableHdfsDirectories = getAvailableDirectoriesFromHeader(
					WebHDFSReaderHandlerConstants.HDFS_PATH);
			if (availableHdfsDirectories == null || availableHdfsDirectories.isEmpty()) {
				return false;
			}
			if (webHdfs1 == null) {
				webHdfs1 = WebHdfsFactory.getWebHdfs(hostNames, port, hdfsUser, authOption);
			}
			for (final String directoryPath : availableHdfsDirectories) {
				final WebHdfsReader webHdfsReader = new WebHdfsReader();

				try {
					List<String> fileNames = webHdfsReader.list(webHdfs1, directoryPath, false);
					for (final String fileName : fileNames) {
						recordsFound = true;
						queueRuntimeInfo(runtimeInfoStore, entityName, fileName);
					}
				} catch (WebHdfsException e) {
					logger.info(getHandlerPhase(), "_message=\"path not found\" directoryPath={} error_message={}",
							directoryPath, e.getMessage());
				}

			}
			logger.info(getHandlerPhase(), "_message=\"initialized runtime info records\" recordsFound={}",
					recordsFound);

			return recordsFound;
		} finally {
			logger.debug(getHandlerPhase(), "releasing webhdfs connection");
			webHdfs1.releaseConnection();
		}
	}

	protected void initRecordToProcess(String nextDescriptorToProcess) throws IOException, WebHdfsException {
		if (webHdfs == null) {
			webHdfs = WebHdfsFactory.getWebHdfs(hostNames, port, hdfsUser, authOption);
		}

		WebHdfsReader webHdfsReader = new WebHdfsReader();

		inputStream = webHdfsReader.getInputStream(webHdfs, nextDescriptorToProcess);

		if (fileChannel != null) { // closing the channel explicitly
			fileChannel.close();
		}
		currentFilePath = nextDescriptorToProcess;
		currentFileStatus = getFileStatusFromWebhdfs(nextDescriptorToProcess);
		fileChannel = Channels.newChannel(inputStream);

		logger.debug(getHandlerPhase(), "current_file_path={} is_file_channel_open={}", currentFilePath,
				fileChannel.isOpen());
	}

	private FileStatus getFileStatusFromWebhdfs(final String hdfsFilePath) throws IOException, WebHdfsException {
		WebHdfs webHdfs1 = null;
		try {
			webHdfs1 = WebHdfsFactory.getWebHdfs(hostNames, port, hdfsUser, authOption);
			WebHdfsReader webHdfsReader = new WebHdfsReader();
			FileStatus fileStatus = webHdfsReader.getFileStatus(webHdfs1, hdfsFilePath);
			return fileStatus;
		} finally {
			webHdfs1.releaseConnection();
		}

	}

	private long getTotalReadFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalSize();
	}

	private boolean readAll() throws HandlerException {
		logger.debug(getHandlerPhase(), "total_read={} total_size={}", getTotalReadFromJournal(),
				getTotalSizeFromJournal());
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}

	private SimpleJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}

}
