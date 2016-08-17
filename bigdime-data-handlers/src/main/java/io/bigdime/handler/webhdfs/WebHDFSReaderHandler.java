package io.bigdime.handler.webhdfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.file.FileInputStreamReaderHandlerConstants;
import io.bigdime.handler.webhdfs.WebHDFSReaderHandlerConfig.READ_HDFS_PATH_FROM;
import io.bigdime.libs.hdfs.FileStatus;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsReader;

/**
 * @formatter:off
 * Read the directory name from the headers.
 * Read the directory listing.
 *    
 * The fileLocation is going to contain multiple files.
 *    
 * readHdfsPathFrom can be headers or config
 * if read
 * 
 * 
 * entityName: user_data
 * readHdfsPathFrom:config
 * hdfsPath: /apps/path1/path2/path3/${yyyy/mm/dd}/user_data
 * touchFileName : /apps/path1/path2/path3/${yyyy/mm/dd}/user_data/_SUCCESS
 * 
 * If touchFileName is present, the process will wait for the touch file to be present.
 * If this field is not present, process will start putting the files immediately.
 * 
 * @formatter:on
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public final class WebHDFSReaderHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(WebHDFSReaderHandler.class));
	private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	private String hdfsFileName;
	private String entityName;
	private WebHDFSPathParser webHDFSPathParser;
	/**
	 * CONFIG or HEADERS
	 */
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	private boolean processingDirty = false;
	private WebHDFSInputDescriptor inputDescriptor;

	private static final String INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:";

	private WebHDFSReaderHandlerConfig handlerConfig = new WebHDFSReaderHandlerConfig();
	private WebHdfsReader webHdfsReader;
	// handlerName:list:directoryPath
	// handlerName:file:filePath

	// hive-jdbc-reader:hiveQuery:
	// Capability to go back

	// webhdfs-file-reader:list:directoryPath
	// webhdfs-file-reader:file:filePath
	// if there are files in start status, process them.
	// If there are directories in start state, process them.
	// Detect the last successful run date, say 100 days back.
	// Check the frequency, say 1 day. Compute next run date, say 99 days back.
	// Look for maxGoBack param. Say, 10 days
	// If today - next run date is < goBack, set next date as goBack
	// else
	// Compute hdfsPath for Today - maxGoBackDays
	// if the descriptor is present and not in QUEUED/START/PENDING for that old
	// date, add frequency to the datetime and compute the next hdfs path.
	// else process for that date.
	/*
	 * @formatter:off
	 * frequency : 1 min | mins | minute | minutes | hour | hours | day | days
	 * frequency will determine when to stop running.
	 * 
	 * goBack :  1 min | mins | minute | minutes | hour | hours | day | days
	 *
	 * Use case: 1
	 * If the frequency is 1 hour, and right now it's 4pm:
	 * If the latency = 1 hour.
	 * timeNow = 4.10 pm
	 * 
	 * processTime = timeNow - frequency - latency
	 * processTime = 2.10pm.
	 * get hour = 2 pm
	 * Set the processing time as 3pm.
	 * 
	 * Use case: 2
	 * If the frequency is 1 day, and right now it's 10th of the month and 4pm:
	 * timeNow = 10th of the month, 4.10 pm
	 * 
	 * processTime = timeNow - frequency
	 * processTime = 9th of the month, 4.10 pm.
	 * get day = 9th of the month
	 * Set the processing time as 9th of the month
	 * 
	 * Use case: 3
	 * If the frequency is 7 days, and right now it's 10th of the month and 4pm:
	 * timeNow = 10th of the month, 4.10 pm
	 * 
	 * processTime = timeNow - 7 days
	 * processTime = 3rd of the month, 4.10 pm.
	 * get day = 3rd of the month
	 * Set the processing time as 3rd of the month
	 * 
	 * 
	 * 
	 * @formatter:on
	 * 
	 * 
	 * (non-Javadoc)
	 * @see io.bigdime.core.handler.AbstractHandler#build()
	 */

	@Override
	public void build() throws AdaptorConfigurationException {
		setHandlerPhase("building WebHDFSReaderHandler");
		super.build();
		try {
			String hostNames = null;
			int port = 0;
			String hdfsPath = null;
			String hdfsUser = null;
			HDFS_AUTH_OPTION authOption = null;
			int bufferSize = 0;
			String readHdfsPathFrom = null;
			String waitForFileName = null;

			logger.info(getHandlerPhase(), "building WebHDFSReaderHandler");

			readHdfsPathFrom = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.READ_HDFS_PATH_FROM);

			if (StringUtils.equalsIgnoreCase(readHdfsPathFrom, "config")) {
				@SuppressWarnings("unchecked")
				Entry<Object, String> srcDescEntry = (Entry<Object, String>) getPropertyMap()
						.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
				logger.info(getHandlerPhase(), "src-desc-node-key=\"{}\" src-desc-node-value=\"{}\"",
						srcDescEntry.getKey(), srcDescEntry.getValue());
				@SuppressWarnings("unchecked")
				Map<String, Object> srcDescValueMap = (Map<String, Object>) srcDescEntry.getKey();

				entityName = PropertyHelper.getStringProperty(srcDescValueMap,
						WebHDFSReaderHandlerConstants.ENTITY_NAME);
				hdfsPath = PropertyHelper.getStringProperty(srcDescValueMap, WebHDFSReaderHandlerConstants.HDFS_PATH);
				getPropertyMap().put(WebHDFSReaderHandlerConstants.ENTITY_NAME, entityName);
			}

			webHDFSPathParser = WebHDFSPathParserFactory.getWebHDFSPathParser(readHdfsPathFrom);

			hostNames = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HOST_NAMES);
			port = PropertyHelper.getIntProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.PORT);

			hdfsUser = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HDFS_USER);
			bufferSize = PropertyHelper.getIntProperty(getPropertyMap(),
					FileInputStreamReaderHandlerConstants.BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
			waitForFileName = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.WAIT_FOR_FILE_NAME);

			final String authChoice = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString());

			authOption = HDFS_AUTH_OPTION.getByName(authChoice);

			logger.info(getHandlerPhase(),
					"hostNames={} port={} hdfsUser={} hdfsPath={} hdfsFileName={} readHdfsPathFrom={}  authChoice={} authOption={} entityName={} webHDFSPathParser={} waitForFileName={}",
					hostNames, port, hdfsUser, hdfsPath, hdfsFileName, readHdfsPathFrom, authChoice, authOption,
					entityName, webHDFSPathParser, waitForFileName);

			handlerConfig.setAuthOption(authOption);
			handlerConfig.setBufferSize(bufferSize);
			handlerConfig.setEntityName(entityName);
			handlerConfig.setHdfsPath(hdfsPath);
			handlerConfig.setHdfsUser(hdfsUser);
			handlerConfig.setHostNames(hostNames);
			handlerConfig.setPort(port);
			handlerConfig.setReadHdfsPathFrom(readHdfsPathFrom);
			handlerConfig.setWaitForFileName(waitForFileName);
			if (getReadHdfsPathFrom() == null) {
				throw new InvalidValueConfigurationException("Invalid value for readHdfsPathFrom: \"" + readHdfsPathFrom
						+ "\" not supported. Supported values are:" + READ_HDFS_PATH_FROM.values());
			}
			webHdfsReader = new WebHdfsReader(hostNames, port, hdfsUser, authOption);
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}

	/**
	 * This method is executed when the handler is run the very first time. Use
	 * it to initialize the connections, find the dirty records etc.
	 * 
	 * @return true if the method completed with success, false otherwise.
	 * @throws RuntimeInfoStoreException
	 */
	protected void initClass() throws RuntimeInfoStoreException {
		if (isFirstRun()) {
			if (getReadHdfsPathFrom() == READ_HDFS_PATH_FROM.HEADERS) {
				entityName = getEntityNameFromHeader();
				logger.info(getHandlerPhase(), "from header, entityName={} ", entityName);
			} else {
				logger.info(getHandlerPhase(), "from config, entityName={} ", entityName);
			}
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
		if (isInputDescriptorNull()) {
			logger.debug(getHandlerPhase(), "returning BACKOFF");
			return io.bigdime.core.ActionEvent.Status.BACKOFF;
		}
		long nextIndexToRead = getTotalReadFromJournal();
		logger.info(getHandlerPhase(),
				"handler_id={} next_index_to_read={} buffer_size={} is_channel_open={} current_file_path={} current_file_size={}",
				getId(), nextIndexToRead, handlerConfig.getBufferSize(), inputDescriptor.getFileChannel().isOpen(),
				inputDescriptor.getCurrentFilePath(), getTotalSizeFromJournal());
		// fileChannel.position(nextIndexToRead);
		final ByteBuffer readInto = ByteBuffer.allocate(getBufferSize());
		Status statustoReturn = Status.READY;

		int bytesRead = inputDescriptor.getFileChannel().read(readInto);
		if (bytesRead > 0) {
			getSimpleJournal().setTotalRead((nextIndexToRead + bytesRead));
			long readCount = getSimpleJournal().getReadCount();
			getSimpleJournal().setReadCount(readCount + 1);
			ActionEvent outputEvent = new ActionEvent();
			byte[] readBody = new byte[bytesRead];
			logger.debug(getHandlerPhase(), "handler_id={} bytes_read={} readBody.length={} fileLength={} readCount={}",
					getId(), bytesRead, readBody.length, inputDescriptor.getCurrentFileStatus().getLength(),
					getSimpleJournal().getReadCount());

			readInto.flip();
			readInto.get(readBody, 0, bytesRead);

			outputEvent.setBody(readBody);
			statustoReturn = Status.CALLBACK;
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME,
					inputDescriptor.getCurrentFilePath());
			outputEvent.getHeaders().put("read_count", "" + getSimpleJournal().getReadCount());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR,
					inputDescriptor.getCurrentFilePath());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.FULL_DESCRIPTOR,
					inputDescriptor.getFullDescriptor());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);

			if (processingDirty)
				outputEvent.getHeaders().put(ActionEventHeaderConstants.CLEANUP_REQUIRED, "true");
			processingDirty = false;// CLEANUP_REQUIRED needs to be done only
									// for the first time

			if (readAll()) {
				logger.info(getHandlerPhase(), "\"read all data\" handler_id={} readCount={} current_file_path={}",
						getId(), getSimpleJournal().getReadCount(), inputDescriptor.getCurrentFilePath());
				getSimpleJournal().reset();
				outputEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.TRUE.toString());
			} else {
				logger.debug(getHandlerPhase(), "\"there is more data to process, returning CALLBACK\" handler_id={}",
						getId());
			}

			processChannelSubmission(outputEvent);
			return statustoReturn;
		} else {
			logger.info(getHandlerPhase(), "returning READY, no data read from the file");
			return Status.READY;
		}
	}

	@Override
	public io.bigdime.core.ActionEvent.Status process() throws HandlerException {

		setHandlerPhase("processing " + getName());
		incrementInvocationCount();
		logger.debug(getHandlerPhase(), "_messagge=\"entering process\" invocation_count={}", getInvocationCount());
		try {
			init(); // initialize cleanup records etc
			// if (readAll()) {
			initDescriptor();
			// }
			if (isInputDescriptorNull()) {
				logger.debug(getHandlerPhase(), "returning BACKOFF");
				return io.bigdime.core.ActionEvent.Status.BACKOFF;
			}
			return doProcess();
		} catch (HandlerException ex) {
			logger.warn(getHandlerPhase(), "_message=\"file not found in hdfs\" error=\"{}\" cause=\"{}\"",
					ex.getMessage(), ex.getCause().getMessage());
			if (ex.getCause().getMessage().equals("Not Found")) {
				logger.warn(getHandlerPhase(), "_message=\"file not found in hdfs, returning backoff\" error={}",
						ex.getMessage());
				return Status.BACKOFF_NOW;
			} else {
				throw ex;
			}
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"error during process", e);
			throw new HandlerException("Unable to process", e);
		} catch (RuntimeInfoStoreException e) {
			throw new HandlerException("Unable to process", e);
		}
	}

	protected void initDescriptor() throws HandlerException, RuntimeInfoStoreException {
		if (readAll()) {
			super.initDescriptor();
			long fileLength = 0;
			if (isInputDescriptorNull())
				fileLength = 0;
			else
				fileLength = inputDescriptor.getCurrentFileStatus().getLength();
			getSimpleJournal().setTotalSize(fileLength);
		}
	}

	protected boolean findAndAddRuntimeInfoRecords() throws RuntimeInfoStoreException, HandlerException {
		boolean recordsFound = false;

		try {
			List<String> availableHdfsDirectories = webHDFSPathParser.parse(getHdfsPath(), getPropertyMap(),
					getHandlerContext().getEventList(), ActionEventHeaderConstants.HDFS_PATH);
			for (final String directoryPath : availableHdfsDirectories) {

				try {

					recordsFound |= initializeRuntimeInfoRecords(directoryPath);

				} catch (IOException | WebHdfsException e) {
					logger.warn(getHandlerPhase(),
							"_message=\"could not initialized runtime info records\" recordsFound={}", recordsFound,
							e.getMessage());
					throw new HandlerException(e);
				}
			}
			logger.info(getHandlerPhase(), "_message=\"initialized runtime info records\" recordsFound={}",
					recordsFound);

			return recordsFound;
		} finally {
			logger.debug(getHandlerPhase(), "releasing webhdfs connection");
		}
	}

	private boolean isReadyFilePresent(final String directoryPath) throws IOException, WebHdfsException {

		if (StringHelper.isBlank(getWaitForFileName())) {
			logger.debug(getHandlerPhase(), "getWaitForFileName() returned {}", getWaitForFileName());
			return true;
		}

		FileStatus fileStatus = webHdfsReader.getFileStatus(directoryPath, getWaitForFileName());
		if (fileStatus != null)
			return true;

		return false;
	}

	private boolean initializeRuntimeInfoRecords(String directoryPath)
			throws RuntimeInfoStoreException, IOException, WebHdfsException {
		boolean recordsFound = false;
		try {
			if (isReadyFilePresent(directoryPath)) {
				List<String> fileNames = webHdfsReader.list(directoryPath, false);
				for (final String fileName : fileNames) {
					Map<String, String> properties = new HashMap<>();
					recordsFound = true;
					properties.put(WebHDFSReaderHandlerConstants.HDFS_PATH, directoryPath);
					properties.put(WebHDFSReaderHandlerConstants.HDFS_FILE_NAME, fileName);
					// queueRuntimeInfo(runtimeInfoStore, entityName,
					// getInputDescriptorPrefix() + fileName, properties);
					WebHDFSInputDescriptor tempInputDescriptor = new WebHDFSInputDescriptor();
					queueRuntimeInfo(runtimeInfoStore, entityName, tempInputDescriptor.createFullDescriptor(fileName),
							properties);
				}
			} else {
				logger.info(getHandlerPhase(), "_message=\"ready file is not present\" waitForFileName={}",
						getWaitForFileName());
			}
		} catch (WebHdfsException e) {
			logger.info(getHandlerPhase(), "_message=\"path not found\" directoryPath={} error_message={}",
					directoryPath, e.getMessage());
		}
		return recordsFound;
	}

	@Override
	protected void initRecordToProcess(RuntimeInfo runtimeInfo) throws HandlerException {
		String webHdfsPathToProcess = null;
		try {
			webHdfsReader.releaseWebHdfsForInputStream();
			String fullDescriptor = runtimeInfo.getInputDescriptor();
			if (inputDescriptor == null)
				inputDescriptor = new WebHDFSInputDescriptor();
			else if (inputDescriptor.getFileChannel() != null) {
				inputDescriptor.getFileChannel().close();
			}

			inputDescriptor.parseDescriptor(fullDescriptor);
			webHdfsPathToProcess = inputDescriptor.getWebhdfsPath();
			final InputStream inputStream = webHdfsReader.getInputStream(webHdfsPathToProcess);

			FileStatus currentFileStatus = getFileStatusFromWebhdfs(inputDescriptor.getWebhdfsPath());
			ReadableByteChannel fileChannel = Channels.newChannel(inputStream);
			inputDescriptor.setCurrentFileStatus(currentFileStatus);
			inputDescriptor.setFileChannel(fileChannel);

			logger.debug(getHandlerPhase(), "current_file_path={} is_file_channel_open={}",
					inputDescriptor.getCurrentFilePath(), fileChannel.isOpen());
		} catch (IOException | WebHdfsException e) {
			runtimeInfo.getProperties().put("error", e.getMessage());
			try {
				logger.debug(getHandlerPhase(), "_message=\"deleting record from runtime info\" runtimeInfo={}",
						runtimeInfo);
				runtimeInfoStore.delete(runtimeInfo);
			} catch (RuntimeInfoStoreException e1) {
				logger.debug(getHandlerPhase(), "_message=\"unable to update runtime info\" file_path={}",
						webHdfsPathToProcess);
			}
			throw new HandlerException("unable to process:" + webHdfsPathToProcess + ", error=" + e.getMessage(), e);
		}
	}

	private FileStatus getFileStatusFromWebhdfs(final String hdfsFilePath) throws IOException, WebHdfsException {

		FileStatus fileStatus = webHdfsReader.getFileStatus(hdfsFilePath);
		return fileStatus;

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

	public String getHostNames() {
		return handlerConfig.getHostNames();
	}

	public int getPort() {
		return handlerConfig.getPort();
	}

	public String getHdfsPath() {
		return handlerConfig.getHdfsPath();
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public HDFS_AUTH_OPTION getAuthOption() {
		return handlerConfig.getAuthOption();
	}

	public READ_HDFS_PATH_FROM getReadHdfsPathFrom() {
		return handlerConfig.getReadHdfsPathFrom();
	}

	public String getHdfsUser() {
		return handlerConfig.getHdfsUser();
	}

	public int getBufferSize() {
		return handlerConfig.getBufferSize();
	}

	private String getWaitForFileName() {
		return handlerConfig.getWaitForFileName();
	}

	protected boolean isInputDescriptorNull() {
		return (inputDescriptor == null || inputDescriptor.getCurrentFilePath() == null)
				|| (inputDescriptor.getCurrentFileStatus().getLength() == 0);
	}

	protected String getInputDescriptorPrefix() {
		return INPUT_DESCRIPTOR_PREFIX;
	}

	@Override
	public void handleException() {
		try {
			getSimpleJournal().reset();
		} catch (HandlerException ex) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"_message=\"handler({}) is unable to handleException\" exception=\"{}\"", getName(),
					ex.getMessage(), ex);
		}
	}

}