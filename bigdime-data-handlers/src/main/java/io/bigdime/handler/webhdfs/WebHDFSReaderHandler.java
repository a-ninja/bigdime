package io.bigdime.handler.webhdfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.file.FileInputStreamReaderHandlerConstants;
import io.bigdime.libs.hdfs.FileStatus;
import io.bigdime.libs.hdfs.WebHDFSConstants;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsReader;
import io.bigdime.libs.hdfs.WebHdfsWithKerberosAuth;

/**
 * @formatter:off
 * Read the directory name from the headers.
 * Read the directory listing.
 * Process like FileInputStreamHandler.
 *    Check if it's a first run.
 *    If it's a first run,
 *    
 *    
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
public class WebHDFSReaderHandler extends AbstractHandler {

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(WebHDFSWriterHandler.class));
	private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	private String hostNames;
	private int port;
	private String hdfsFileName;
	private String hdfsPath;
	private String hdfsUser;
	private WebHdfs webHdfs;
	private String entityName;

	private int bufferSize;

	private String readHdfsPathFrom; // CONFIG | HEADERS

	private FileStatus currentFileStatus;
	private String currentFilePath;
	private long fileLength = -1;
	ReadableByteChannel fileChannel;

	private InputStream inputStream;
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	private String handlerPhase;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		try {
			handlerPhase = "building WebHDFSReaderHandler";
			logger.info(handlerPhase, "building WebHDFSReaderHandler");
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

			logger.info(handlerPhase, "hdfsUser={} hdfsPath={} readHdfsPathFrom={}", hdfsUser, hdfsPath,
					readHdfsPathFrom);

			logger.info(handlerPhase, "hostNames={} port={} hdfsFileName={} hdfsPath={} hdfsUser={}", hostNames, port,
					hdfsFileName, hdfsPath, hdfsUser);
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}

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
	public Status process() throws HandlerException {

		handlerPhase = "processing WebHDFSReaderHandler";
		incrementInvocationCount();
		logger.debug(handlerPhase, "_messagge=\"entering process\" invocation_count={}", getInvocationCount());
		try {
			Status status = preProcess();
			if (status == Status.BACKOFF) {
				logger.debug(handlerPhase, "returning BACKOFF");
				return status;
			}
			return doProcess();
		} catch (IOException | WebHdfsException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"error during reading file", e);
			throw new HandlerException("Unable to process message from file", e);
		} catch (RuntimeInfoStoreException e) {
			throw new HandlerException("Unable to process message from file", e);
		}
	}

	// boolean ranOnce = false;

	private Status doProcess() throws IOException, HandlerException, RuntimeInfoStoreException {
		long nextIndexToRead = getTotalReadFromJournal();
		logger.debug(handlerPhase,
				"handler_id={} next_index_to_read={} buffer_size={} is_channel_open={} current_file_path={}", getId(),
				nextIndexToRead, bufferSize, fileChannel.isOpen(), currentFilePath);
		// fileChannel.position(nextIndexToRead);
		final ByteBuffer readInto = ByteBuffer.allocate(bufferSize);
		Status statustoReturn = Status.READY;
		/////////////////

		// logger.debug(handlerPhase, "handler_id={} ranOnce={} readAll={}",
		// getId(), ranOnce, readAll());
		// if (ranOnce) {
		// try {
		// Thread.sleep(5000);
		// return Status.READY;
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// final byte[] readBody = new byte[bufferSize];
		// int bytesRead = inputStream.read(readBody);
		int bytesRead = fileChannel.read(readInto);
		logger.debug(handlerPhase, "handler_id={} bytes_read={}", getId(), bytesRead);
		if (bytesRead > 0) {
			getSimpleJournal().setTotalRead((nextIndexToRead + bytesRead));
			long readCount = getSimpleJournal().getReadCount();
			getSimpleJournal().setReadCount(readCount + 1);
			ActionEvent outputEvent = new ActionEvent();
			byte[] readBody = new byte[bytesRead];
			logger.debug(handlerPhase, "handler_id={} readBody.length={} fileLength={} readCount={}", getId(),
					readBody.length, fileLength, getSimpleJournal().getReadCount());

			readInto.flip();
			readInto.get(readBody, 0, bytesRead);
			// outputEvent.setBody(readBody);

			outputEvent.setBody(readBody);
			statustoReturn = Status.CALLBACK;
			if (readAll()) {
				logger.debug(handlerPhase, "\"read all data\" handler_id={} readCount={}", getId(),
						getSimpleJournal().getReadCount());
				getSimpleJournal().reset();
				outputEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
				// ranOnce = true;
			} else {
				logger.debug(handlerPhase, "\"there is more data to process, returning CALLBACK\" handler_id={}",
						getId());
			}
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME, currentFilePath);
			outputEvent.getHeaders().put("read_count", "" + getSimpleJournal().getReadCount());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, currentFilePath);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);

			logger.debug(handlerPhase, "_message=\"checking process submission, headers={}\" handler_id={}",
					outputEvent.getHeaders(), getId());
			processChannelSubmission(outputEvent);
			return statustoReturn;
		} else {
			logger.debug(handlerPhase, "returning READY, no data read from the file");
			return Status.READY;
		}

		//////////////

	}

	private Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException, WebHdfsException {
		if (isFirstRun()) {

			entityName = getEntityNameFromHeader();
			logger.info(handlerPhase, "From header, entityName={} ", entityName);
			// dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore,
			// entityName);
			// if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
			// dirtyRecordCount = dirtyRecords.size();
			// logger.warn(handlerPhase,
			// "_message=\"dirty records found\" handler_id={}
			// dirty_record_count=\"{}\" entityName={}",
			// getId(), dirtyRecordCount, entityName);
			// } else {
			// logger.info(handlerPhase, "_message=\"no dirty records found\"
			// handler_id={}", getId());
			// }
		}
		if (readAll()) {
			setNextFileToProcess();
			if (currentFilePath == null) {
				logger.info(handlerPhase, "_message=\"no file to process\" handler_id={} descriptor={}", getId());
				return Status.BACKOFF;
			}

			// if (webHdfs == null) {
			// webHdfs = WebHdfs.getInstance(hostNames, port)
			// .addHeader(WebHDFSConstants.CONTENT_TYPE,
			// WebHDFSConstants.APPLICATION_OCTET_STREAM)
			// .addParameter(WebHDFSConstants.USER_NAME, hdfsUser);
			// }
			// final WebHdfsReader webHdfsReader = new WebHdfsReader();
			// webHdfsReader.getFileStatus(webHdfs, currentFilePath);

			fileLength = currentFileStatus.getLength();
			logger.info(handlerPhase, "_message=\"got a new file to process\" handler_id={} file_length={}", getId(),
					fileLength);
			if (fileLength == 0) {
				logger.info(handlerPhase, "_message=\"file is empty\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			getSimpleJournal().setTotalSize(fileLength);
			// fileChannel = file.getChannel();
			// return Status.READY;
		}
		return Status.READY;
	}

	protected void setNextFileToProcess()
			throws IOException, RuntimeInfoStoreException, HandlerException, WebHdfsException {

		// if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
		// RuntimeInfo dirtyRecord = dirtyRecords.remove(0);
		// logger.info(handlerPhase, "\"processing a dirty record\"
		// dirtyRecord=\"{}\"", dirtyRecord);
		// String nextDescriptorToProcess = dirtyRecord.getInputDescriptor();
		// initFile(nextDescriptorToProcess);
		// processingDirty = true;
		// return;
		// } else {
		logger.info(handlerPhase, "processing a clean record");
		// processingDirty = false;
		RuntimeInfo queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName);
		if (queuedRecord == null) {
			boolean foundRecordsOnDisk = initializeRuntimeInfoRecords();
			if (foundRecordsOnDisk)
				queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName);
		}
		if (queuedRecord != null) {
			logger.info(handlerPhase, "_message=\"found a queued record\" queued_record={}", queuedRecord);
			initFile(queuedRecord.getInputDescriptor());
			Map<String, String> properties = new HashMap<>();
			properties.put("handlerName", this.getClass().getName());
			updateRuntimeInfo(runtimeInfoStore, entityName, queuedRecord.getInputDescriptor(),
					RuntimeInfoStore.Status.STARTED, properties);
		} else {
			// file = null;
		}
		// }
	}

	private boolean initializeRuntimeInfoRecords() throws RuntimeInfoStoreException, IOException, WebHdfsException {
		WebHdfs webHdfs1 = null;
		try {
			List<String> availableHdfsDirectories = getAvailableDirectoriesFromHeader(
					WebHDFSReaderHandlerConstants.HDFS_PATH);
			if (availableHdfsDirectories == null || availableHdfsDirectories.isEmpty()) {
				return false;
			}
			if (webHdfs1 == null) {
				webHdfs1 = WebHdfsWithKerberosAuth.getInstance(hostNames, port).addHeader(WebHDFSConstants.CONTENT_TYPE,
						WebHDFSConstants.APPLICATION_OCTET_STREAM);
			}
			for (String directoryPath : availableHdfsDirectories) {
				final WebHdfsReader webHdfsReader = new WebHdfsReader();

				final List<String> fileNames = webHdfsReader.list(webHdfs1, directoryPath, false);

				for (final String fileName : fileNames) {
					queueRuntimeInfo(runtimeInfoStore, entityName, fileName);
				}
			}
			return true;
		} finally {
			webHdfs1.releaseConnection();
			logger.debug(handlerPhase, "releasing webhdfs connection");
		}
	}

	private void initFile(String nextDescriptorToProcess) throws IOException, WebHdfsException {
		if (webHdfs == null) {
			webHdfs = WebHdfsWithKerberosAuth.getInstance(hostNames, port).addHeader(WebHDFSConstants.CONTENT_TYPE,
					WebHDFSConstants.APPLICATION_OCTET_STREAM);
		}

		WebHdfsReader webHdfsReader = new WebHdfsReader();

		inputStream = webHdfsReader.getInputStream(webHdfs, nextDescriptorToProcess);

		if (fileChannel != null) { // closing the channel explicitly
			fileChannel.close();
		}
		currentFilePath = nextDescriptorToProcess;
		currentFileStatus = getFileStatusFromWebhdfs(nextDescriptorToProcess);
		fileChannel = Channels.newChannel(inputStream);

		logger.debug(handlerPhase, "current_file_path={} is_channel_open={}", currentFilePath, fileChannel.isOpen());
	}

	private FileStatus getFileStatusFromWebhdfs(final String hdfsFilePath) throws IOException, WebHdfsException {
		WebHdfs webHdfs1 = null;
		try {
			webHdfs1 = WebHdfsWithKerberosAuth.getInstance(hostNames, port).addHeader(WebHDFSConstants.CONTENT_TYPE,
					WebHDFSConstants.APPLICATION_OCTET_STREAM);
			WebHdfsReader webHdfsReader = new WebHdfsReader();
			FileStatus fileStatus = webHdfsReader.getFileStatus(webHdfs1, hdfsFilePath);
			return fileStatus;
		} finally {
			webHdfs1.releaseConnection();
		}

	}

	/*
	 * private Status process0(final ActionEvent actionEvent) throws
	 * HandlerException, IOException, WebHdfsException { // if (webHdfs == null)
	 * { // webHdfs = WebHdfs.getInstance(hostNames, port) //
	 * .addHeader(WebHDFSConstants.CONTENT_TYPE, //
	 * WebHDFSConstants.APPLICATION_OCTET_STREAM) //
	 * .addParameter(WebHDFSConstants.USER_NAME, hdfsUser); // }
	 * 
	 * if (readHdfsPathFrom.equalsIgnoreCase("headers")) { hdfsPath =
	 * actionEvent.getHeaders().get(WebHDFSReaderHandlerConstants.HDFS_PATH);
	 * logger.info(handlerPhase, "From headers, hdfsPath={}", hdfsPath);
	 * hdfsPath = "/webhdfs/v1" + hdfsPath; logger.info(handlerPhase,
	 * "From headers, hdfsPathWithWebhdfs={}", "/webhdfs/v1" + hdfsPath); } //
	 * WebHdfsReader webHdfsReader = new WebHdfsReader();
	 * 
	 * // List<String> fileNames = webHdfsReader.list(webHdfs, hdfsPath, //
	 * false); // logger.warn(handlerPhase, "directory={} files={}", hdfsPath,
	 * // fileNames);
	 * 
	 * // InputStream inputStream = webHdfsReader.getInputStream(webHdfs, //
	 * hdfsPath); // ReadableByteChannel channel =
	 * Channels.newChannel(inputStream); final ByteBuffer readInto =
	 * ByteBuffer.allocate(bufferSize);
	 * 
	 * int bytesRead = fileChannel.read(readInto); if (bytesRead > 0) {
	 * ActionEvent outputEvent = new ActionEvent(); byte[] readBody = new
	 * byte[bytesRead];
	 * 
	 * readInto.flip(); readInto.get(readBody, 0, bytesRead);
	 * outputEvent.setBody(readBody);
	 * 
	 * outputEvent.setBody(readBody);
	 * 
	 * if (readAll()) { getSimpleJournal().reset(); //
	 * logger.debug(handlerPhase, // "_message=\"done reading file={}, there
	 * might be more files // to process, returning CALLBACK\" handler_id={} //
	 * headers_from_file_handler={}", // currentFile.getAbsolutePath(), getId(),
	 * // outputEvent.getHeaders());
	 * outputEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE,
	 * Boolean.TRUE.toString()); return Status.CALLBACK; } else {
	 * logger.debug(handlerPhase,
	 * "\"there is more data to process, returning CALLBACK\" handler_id={}",
	 * getId()); return Status.CALLBACK; } } else { logger.debug(handlerPhase,
	 * "returning READY, no data read from the file"); return Status.READY; }
	 * 
	 * }
	 */
	private long getTotalReadFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalSize();
	}

	private boolean readAll() throws HandlerException {
		logger.debug(handlerPhase, "total_read={} total_size={}", getTotalReadFromJournal(), getTotalSizeFromJournal());
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}

	private SimpleJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}

}
