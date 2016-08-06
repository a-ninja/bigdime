/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

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
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.commons.FileHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.handler.HandlerJournal;
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
 * @author Neeraj Jain, Rita Liu
 *
 */
@Component
@Scope("prototype")
public class ZipFileInputStreamHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(ZipFileInputStreamHandler.class));
	public static final String FILE_LOCATION = "fileLocation";

	private static int DEFAULT_BUFFER_SIZE = 1024 * 1024;

	private File currentFile;

	private String fileNamePattern;

	private int bufferSize;

	/**
	 * Base path, absolute, to look for the files.
	 */
	private String fileLocation;

	private long zipFileLength = -1;

	private FileInputDescriptor fileInputDescriptor;

	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;
	@Autowired
	private FileHelper fileHelper;

	private String handlerPhase = "";
	private String entityName;

	private File currentFileToProcess;
	private int readEntries = 0;
	private String basePath = "/";
	private boolean preserveBasePath;
	private boolean preserveRelativePath;
	private ZipEntry zipFileEntry;
	private ZipFile zipFile;
	private InputStream inputStream;
	private BufferedInputStream bufferedInputStream;
	private int noOfEntry;
	private String fileName = "";
	private long totalRead = 0;
	private long fileSize = 0;
	Enumeration<? extends ZipEntry> entriesInZip;
		
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building ZipFileInputStreamHandler";
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
				DataConstants.SLASH);
		logger.debug(handlerPhase, "basePath={}", basePath);

		fileInputDescriptor = new FileInputDescriptor();
		try {
			fileInputDescriptor.parseDescriptor(basePath);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"src-desc must contain entityName:fileNamePrefix, e.g. user:web_user*");
		}

		fileLocation = fileInputDescriptor.getPath();
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
		handlerPhase = "processing ZipFileInputStreamHandler";
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
	
	long dirtyRecordCount = 0;
	List<RuntimeInfo> dirtyRecords;
	private boolean processingDirty = false;
	
	private void getStartedRecordsFromRuntimeInfos() throws RuntimeInfoStoreException{
		dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, entityName);
		if(dirtyRecords != null && !dirtyRecords.isEmpty()){
			dirtyRecordCount = dirtyRecords.size();
			logger.warn(handlerPhase,
					"_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entityName={}",
					getId(), dirtyRecordCount, entityName);
		} else {
			logger.info(handlerPhase, "_message=\"no dirty records found\" handler_id={}", getId());
		}
	}

	@Override
	protected Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException {
		
		if(isFirstRun()){
			getStartedRecordsFromRuntimeInfos();
		}
		
		if (readAllFromZip() && readAllFromFile()) {
			currentFileToProcess = getNextFileToProcess();
			if (currentFileToProcess == null) {			
				logger.info(handlerPhase, "_message=\"no file to process\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			noOfEntry = getNumberOfFileInZip(currentFileToProcess);
			readEntries++;
			zipFileLength = currentFileToProcess.length();
			logger.info(handlerPhase, "_message=\"got a new file to process\" handler_id={} zip_file_length={}", getId(),
					zipFileLength);
			if (zipFileLength == 0) {
				logger.info(handlerPhase, "_message=\"zip file is empty\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
			logger.info(handlerPhase, "_message=\"number files in zip\" handler_id={} no_of_file={} ", getId(),noOfEntry);
			getZipFileHandlerJournal().setTotalEntries(noOfEntry);
			getZipFileHandlerJournal().setZipFileName(currentFileToProcess.getName());
			getZipFileHandlerJournal().setReadEntries(readEntries);
			return Status.READY;
		}
		return Status.READY;
	}

	@Override
	protected Status doProcess() throws IOException, HandlerException, RuntimeInfoStoreException {
		
		byte[] data ;
		long bytesReadCount=0;
		int readSize =0;
		String srcFileName = "";
		
		logger.debug(handlerPhase, "handler_id={} next_index_to_read={} buffer_size={}", getId(), getTotalReadFromJournal(),
				bufferSize);
		if(getReadEntriesFromJournal() == 1 && readAllFromFile()){
			entriesInZip = zipFile.entries();
		}
		if(readAllFromFile()){
			fileName = fileNameFromZip(entriesInZip);
			if(fileName.contains(DataConstants.SLASH)){
				srcFileName = fileName.substring(fileName.lastIndexOf(DataConstants.SLASH)+1);	
			}else{
				srcFileName = fileName;
			}
			getZipFileHandlerJournal().setEntryName(fileName);
			getZipFileHandlerJournal().setSrcFileName(srcFileName);
			getZipFileHandlerJournal().setTotalSize(zipFileEntry.getSize());
			inputStream = zipFile.getInputStream(zipFileEntry);
			bufferedInputStream = new BufferedInputStream(inputStream);
			fileSize = zipFileEntry.getSize();
			if (fileSize == 0) {
				logger.info(handlerPhase, "_message=\"file is empty\" handler_id={} ", getId());
				return Status.BACKOFF;
			}
		}
		
		logger.info(
				handlerPhase,
				"_message=\"File details\" handler_id={}  file_name={} file_size={} ",
				getId(), fileName, fileSize);
		
		if(fileSize < bufferSize){
			readSize = (int) fileSize;
		}else{
			readSize = bufferSize;
		}
		
		data = new byte[readSize];
		bytesReadCount = bufferedInputStream.read(data, 0, readSize);
		if(bytesReadCount > 0){
			totalRead = totalRead + bytesReadCount;
			getZipFileHandlerJournal().setTotalRead(totalRead);
		
			ActionEvent outputEvent = new ActionEvent();
			logger.debug(handlerPhase,
				"handler_id={} readBody.length={} remaining={}", getId(),
				bytesReadCount, (fileSize - bytesReadCount));

			outputEvent.setBody(data);
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.INPUT_DESCRIPTOR,
				currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_PATH,
				currentFile.getAbsolutePath());
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_NAME,
				getZipFileHandlerJournal().getSrcFileName());
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_TOTAL_SIZE,
				String.valueOf(getTotalSizeFromJournal()));
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_TOTAL_READ,
				String.valueOf(getTotalReadFromJournal()));
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.SOURCE_FILE_LOCATION,
				currentFile.getParent());
			outputEvent.getHeaders().put(ActionEventHeaderConstants.BASE_PATH,
				basePath);
			String relativeToBasePath = "";
			if (currentFile.getParent().length() > basePath.length()){
				relativeToBasePath = currentFile.getParent().substring(
					basePath.length());
			}
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.RELATIVE_PATH,
				relativeToBasePath);
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.PRESERVE_BASE_PATH,
				String.valueOf(preserveBasePath));
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.PRESERVE_RELATIVE_PATH,
				String.valueOf(preserveRelativePath));
			logger.debug(handlerPhase, "setting entityName header, value={}",
				entityName);
			outputEvent.getHeaders().put(
				ActionEventHeaderConstants.ENTITY_NAME, entityName);	
			logger.debug(handlerPhase, "setting CLEANUP_REQUIRED header, processingDirty={} ", processingDirty);
			if (processingDirty){
				outputEvent.getHeaders().put(ActionEventHeaderConstants.CLEANUP_REQUIRED, "true");
			}
			processingDirty = false;	
			outputEvent.getHeaders().put(
					ActionEventHeaderConstants.READ_COMPLETE,
					Boolean.FALSE.toString());
			
			getHandlerContext().createSingleItemEventList(outputEvent);

			processChannelSubmission(outputEvent);
			
			if(!readAllFromFile() &&
				getZipFileHandlerJournal().getEntryName().equalsIgnoreCase(fileName)){
				fileSize = fileSize - bytesReadCount;
				bytesReadCount = 0;
				return Status.CALLBACK;
			} else if(!getZipFileHandlerJournal().getEntryName().equalsIgnoreCase(fileName)){
				logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"file name mismatch during read file");
				throw new HandlerException("file name is not same as file name in Journal");
			} else{
				if(readEntries == noOfEntry){		
					logger.debug(handlerPhase, "returning READY, no file need read from the zip");
					logger.debug(handlerPhase,
						"_message=\"handler read data, ready to return\", context.list_size={} total_read={} total_size={} context={} fileSize={}",
						getHandlerContext().getEventList().size(), getTotalReadFromJournal(), getTotalSizeFromJournal(),
						getHandlerContext(), getZipFileHandlerJournal().getTotalSize());
					totalRead = 0;
					getZipFileHandlerJournal().reset();
					readEntries = 0;
					outputEvent.getHeaders().put(
						ActionEventHeaderConstants.READ_COMPLETE,
						Boolean.TRUE.toString());
					return Status.READY;
				}else{
					logger.debug(
						handlerPhase,
						"_message=\"done reading file={}, there might be more files to process, returning CALLBACK\" handler_id={} headers_from_file_handler={}",
						currentFile.getAbsolutePath(), getId(),
						outputEvent.getHeaders());
					readEntries++;
					getZipFileHandlerJournal().setTotalRead(0);
					getZipFileHandlerJournal().setTotalSize(0);
					getZipFileHandlerJournal().setEntryName(null);
					getZipFileHandlerJournal().setSrcFileName(null);
					return Status.CALLBACK;
				}
			}
		}else {
			logger.debug(handlerPhase, "returning READY, no data read from the file");
			return Status.READY;
		}		
	}

	private File getQueuedRecordsFromRuntimeInfos() throws RuntimeInfoStoreException, IOException {
		RuntimeInfo queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName);
		if (queuedRecord == null) {
			boolean foundRecordsOnDisk = findAndAddRuntimeInfoRecords();
			if (foundRecordsOnDisk)
				queuedRecord = getOneQueuedRuntimeInfo(runtimeInfoStore, entityName);
		}
		if(queuedRecord != null){
			File file = new File(queuedRecord.getInputDescriptor());
			logger.debug(handlerPhase, "absolute_path={} file_name_for_descriptor={}",
					file.getAbsolutePath(), file.getName());
			updateRuntimeInfo(runtimeInfoStore, entityName, queuedRecord.getInputDescriptor(),
					RuntimeInfoStore.Status.STARTED);
			return file;
		}else{
			return null;
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
	
	protected File getNextFileToProcess()
			throws RuntimeInfoStoreException, HandlerException, IOException {
		if(dirtyRecords != null && !dirtyRecords.isEmpty()) {
			RuntimeInfo dirtyRecord = dirtyRecords.remove(0);
			logger.info(handlerPhase, "\"processing a dirty record\" dirtyRecord=\"{}\"", dirtyRecord);
			String nextDescriptorToProcess = dirtyRecord.getInputDescriptor();
			currentFile = new File(nextDescriptorToProcess);
			logger.debug(handlerPhase, "absolute_path={} file_name_for_descriptor={}",
					currentFile.getAbsolutePath(), currentFile.getName());
			processingDirty = true;
		} else {
			logger.info(handlerPhase, "processing a clean record");
			processingDirty = false;
			currentFile = getQueuedRecordsFromRuntimeInfos();			
		}
		return currentFile;
	}

	
	
	private int getNumberOfFileInZip(File currentZipFile) {
		try {
			zipFile = new ZipFile(currentZipFile);
			return zipFile.size();
		} catch (ZipException e) {
			logger.warn(handlerPhase, "_message=\"Unable to open zip file\" currentZipFile={}",
					currentZipFile, e);
		} catch (IOException e) {
			logger.warn(handlerPhase, "_message=\"error during getting number of files in zip\" currentZipFile={}", 
					currentZipFile, e);
		}
		return 0;
	}
	
	private String fileNameFromZip(Enumeration<? extends ZipEntry> Zentries){
		zipFileEntry = Zentries.nextElement();
		return zipFileEntry.getName();
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
		HandlerJournal journal = getZipFileHandlerJournal();
		return journal.getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		HandlerJournal journal = getZipFileHandlerJournal();
		return journal.getTotalSize();
	}
	
	private int getNoOfEntriesFromJournal() throws HandlerException {
		return getZipFileHandlerJournal().getTotalEntries();
	}
	
	private int getReadEntriesFromJournal() throws HandlerException {
		return getZipFileHandlerJournal().getReadEntries();
	}

	private ZipFileHandlerJournal getZipFileHandlerJournal() throws HandlerException {
		ZipFileHandlerJournal journal =	getNonNullJournal(ZipFileHandlerJournal.class);
		return journal;
	}

	private boolean readAllFromFile() throws HandlerException {
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}
	
	private boolean readAllFromZip() throws HandlerException {
		if (getReadEntriesFromJournal() == getNoOfEntriesFromJournal()) {
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
