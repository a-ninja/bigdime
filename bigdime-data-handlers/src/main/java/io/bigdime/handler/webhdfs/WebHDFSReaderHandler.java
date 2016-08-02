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

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
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
 * readHdfsPathFrom can be headers or config
 * if read
 * 
 * 
 * entityName: user_data
 * readHdfsPathFrom:config
 * hdfsPath: /apps/path1/path2/path3/${yyyy/mm/dd}/user_data
 * touchFileCheck : true|false requiredField
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
public class WebHDFSReaderHandler extends AbstractSourceHandler {

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(WebHDFSReaderHandler.class));
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
	private WebHDFSPathParser webHDFSPathParser;
	/**
	 * CONFIG or HEADERS
	 */
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
	private final String PATH_INPUT_DESCRIPTOR_PREFIX = "::/webhdfs/v1/";

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

	@Override
	public void build() throws AdaptorConfigurationException {
		setHandlerPhase("building WebHDFSReaderHandler");
		super.build();
		try {
			logger.info(getHandlerPhase(), "building WebHDFSReaderHandler");

			readHdfsPathFrom = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.READ_HDFS_PATH_FROM);

			if (StringUtils.equalsIgnoreCase(readHdfsPathFrom, "config")) {
				@SuppressWarnings("unchecked")
				Entry<Object, String> srcDescEntry = (Entry<Object, String>) getPropertyMap()
						.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
				logger.debug(getHandlerPhase(), "src-desc-node-key=\"{}\" src-desc-node-value=\"{}\"",
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

			final String authChoice = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString());

			authOption = HDFS_AUTH_OPTION.getByName(authChoice);

			logger.info(getHandlerPhase(),
					"hostNames={} port={} hdfsUser={} hdfsPath={} hdfsFileName={} readHdfsPathFrom={}  authChoice={} authOption={} entityName={} webHDFSPathParser={}",
					hostNames, port, hdfsUser, hdfsPath, hdfsFileName, readHdfsPathFrom, authChoice, authOption,
					entityName, webHDFSPathParser);
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
	protected boolean init() throws RuntimeInfoStoreException {
		if (isFirstRun()) {
			if (readHdfsPathFrom.equals("headers")) {
				entityName = getEntityNameFromHeader();
				logger.info(getHandlerPhase(), "From header, entityName={} ", entityName);
			} else {
				logger.info(getHandlerPhase(), "From config, entityName={} ", entityName);
			}
			dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, entityName, INPUT_DESCRIPTOR_PREFIX);

			if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
				dirtyRecordCount = dirtyRecords.size();
				logger.warn(getHandlerPhase(),
						"_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entityName={}",
						getId(), dirtyRecordCount, entityName);
			} else {
				logger.info(getHandlerPhase(), "_message=\"no dirty records found\" handler_id={}", getId());
			}
		}
		return false;
	}

	/**
	 * gets called before process method execution every time.
	 * 
	 * 
	 */
	protected void pre() {

	}

	/**
	 * gets called after process method execution every time.
	 * 
	 * 
	 */

	protected void post() {

	}

	/**
	 * Gets called after pre and before post method execution every time.
	 */
	protected void execute() {

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
		if (bytesRead > 0) {
			getSimpleJournal().setTotalRead((nextIndexToRead + bytesRead));
			long readCount = getSimpleJournal().getReadCount();
			getSimpleJournal().setReadCount(readCount + 1);
			ActionEvent outputEvent = new ActionEvent();
			byte[] readBody = new byte[bytesRead];
			logger.debug(getHandlerPhase(), "handler_id={} bytes_read={} readBody.length={} fileLength={} readCount={}",
					getId(), bytesRead, readBody.length, fileLength, getSimpleJournal().getReadCount());

			readInto.flip();
			readInto.get(readBody, 0, bytesRead);

			outputEvent.setBody(readBody);
			statustoReturn = Status.CALLBACK;
			outputEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME, currentFilePath);
			outputEvent.getHeaders().put("read_count", "" + getSimpleJournal().getReadCount());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, currentFilePath);
			outputEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME, entityName);

			if (processingDirty)
				outputEvent.getHeaders().put(ActionEventHeaderConstants.CLEANUP_REQUIRED, "true");
			processingDirty = false;// CLEANUP_REQUIRED needs to be done only
									// for the first time

			if (readAll()) {
				logger.info(getHandlerPhase(), "\"read all data\" handler_id={} readCount={} current_file_path={}",
						getId(), getSimpleJournal().getReadCount(), currentFilePath);
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
	protected Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException {
		init();

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
			if (webHdfs1 == null) {
				webHdfs1 = WebHdfsFactory.getWebHdfs(hostNames, port, hdfsUser, authOption);
			}
			List<String> availableHdfsDirectories = webHDFSPathParser.parse(hdfsPath, getPropertyMap(),
					getHandlerContext().getEventList(), ActionEventHeaderConstants.HDFS_PATH);
			for (final String directoryPath : availableHdfsDirectories) {
				recordsFound |= initializeRuntimeInfoRecords(webHdfs1, directoryPath);
			}
			logger.info(getHandlerPhase(), "_message=\"initialized runtime info records\" recordsFound={}",
					recordsFound);

			return recordsFound;
		} finally {
			logger.debug(getHandlerPhase(), "releasing webhdfs connection");
			webHdfs1.releaseConnection();
		}
	}

	private boolean initializeRuntimeInfoRecords(WebHdfs webHdfs1, String directoryPath)
			throws RuntimeInfoStoreException, IOException, WebHdfsException {
		boolean recordsFound = false;
		final WebHdfsReader webHdfsReader = new WebHdfsReader();

		try {
			List<String> fileNames = webHdfsReader.list(webHdfs1, directoryPath, false);
			for (final String fileName : fileNames) {
				Map<String, String> properties = new HashMap<>();
				recordsFound = true;
				properties.put(WebHDFSReaderHandlerConstants.HDFS_PATH, directoryPath);
				properties.put(WebHDFSReaderHandlerConstants.HDFS_FILE_NAME, fileName);
				queueRuntimeInfo(runtimeInfoStore, entityName, fileName, properties);
			}

		} catch (WebHdfsException e) {
			logger.info(getHandlerPhase(), "_message=\"path not found\" directoryPath={} error_message={}",
					directoryPath, e.getMessage());
		}
		return recordsFound;
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

	public String getHostNames() {
		return hostNames;
	}

	public void setHostNames(String hostNames) {
		this.hostNames = hostNames;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public HDFS_AUTH_OPTION getAuthOption() {
		return authOption;
	}

	public void setAuthOption(HDFS_AUTH_OPTION authOption) {
		this.authOption = authOption;
	}

	public String getReadHdfsPathFrom() {
		return readHdfsPathFrom;
	}

	public void setReadHdfsPathFrom(String readHdfsPathFrom) {
		this.readHdfsPathFrom = readHdfsPathFrom;
	}

}
