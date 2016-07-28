package io.bigdime.handler.webhdfs;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.CollectionUtil;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsFactory;
import io.bigdime.libs.hdfs.WebHdfsReader;

@Component
@Scope("prototype")
public class WebHDFSDirectoryListingReaderHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(WebHDFSDirectoryListingReaderHandler.class));
	private String hostNames;
	private int port;
	private String hdfsFileName;
	private String hdfsPath;
	private String hdfsUser;
	private HDFS_AUTH_OPTION authOption;

	// private String readHdfsPathFrom; // CONFIG | HEADERS

	ReadableByteChannel fileChannel;

	protected List<RuntimeInfo> dirtyRecords;
	public static final String FORWARD_SLASH = "/";

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		try {
			setHandlerPhase("building WebHDFSReaderHandler");
			logger.info(getHandlerPhase(), "building WebHDFSReaderHandler");
			hostNames = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HOST_NAMES);
			port = PropertyHelper.getIntProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.PORT);

			hdfsUser = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSReaderHandlerConstants.HDFS_USER);
			// readHdfsPathFrom =
			// PropertyHelper.getStringProperty(getPropertyMap(),
			// WebHDFSReaderHandlerConstants.READ_HDFS_PATH_FROM);
			// if (StringUtils.equalsIgnoreCase(readHdfsPathFrom, "config")) {
			// hdfsPath = PropertyHelper.getStringProperty(getPropertyMap(),
			// WebHDFSReaderHandlerConstants.HDFS_PATH);
			// }

			final String authChoice = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSReaderHandlerConstants.AUTH_CHOICE, HDFS_AUTH_OPTION.KERBEROS.toString());

			authOption = HDFS_AUTH_OPTION.getByName(authChoice);
			logger.info(getHandlerPhase(),
					"hostNames={} port={} hdfsUser={} hdfsPath={} hdfsFileName={}  authChoice={} authOption={}",
					hostNames, port, hdfsUser, hdfsPath, hdfsFileName, authChoice, authOption);
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}

	/**
	 * @formatter:off
	 * Read the list of events.
	 * For each event, get the header value for key = "hdfsPath".
	 * For each hdfsPath, get the list of files from hdfs using webhdfs.
	 * 
	 * Create an output event, retain all the headers from input event.
	 * Add a new header as sourceFileName, the value may be touchfiles/date__entity
	 * Set the body to be the list of files and write the bytes.
	 * If there are more directories available, set journal, return CALLBACK.
	 * Return READY otherwise.
	 * 
	 * @formatter:on
	 * @return
	 * @throws RuntimeInfoStoreException
	 * @throws IOException
	 * @throws WebHdfsException
	 * @throws HandlerException 
	 */

	@Override
	public Status process() throws HandlerException {

		setHandlerPhase("processing WebHDFSDirectoryListingReaderHandler");
		incrementInvocationCount();

		HandlerJournal journal = getJournal();

		Status returnStatus = Status.READY;

		if (journal == null || journal.getEventList() == null) {
			// process for ready status.
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
			logger.info(getHandlerPhase(), "journal is null, actionEvents.size={} id={} ", actionEvents.size(),
					getId());
			if (actionEvents.isEmpty())
				returnStatus = Status.BACKOFF;
			else
				returnStatus = process0(actionEvents);

		} else {
			List<ActionEvent> actionEvents = journal.getEventList();
			logger.info(getHandlerPhase(), "journal is not null, actionEvents is nullOrEmpty={}",
					CollectionUtil.isEmpty(actionEvents));
			if (CollectionUtil.isEmpty(actionEvents)) {
				returnStatus = process0(journal.getEventList());
			} else {
				returnStatus = Status.BACKOFF;
			}
		}
		processLastHandler();
		return returnStatus;
	}

	protected HandlerJournal getJournal() throws HandlerException {
		return getSimpleJournal();
	}

	protected Status process0(List<ActionEvent> eventList) throws HandlerException {
		HandlerJournal journal = getSimpleJournal();
		ActionEvent inputEvent = eventList.remove(0);

		logger.debug(getHandlerPhase(), "_message=\"entering process0\" headers={}", inputEvent.getHeaders());
		String sourceFileName = inputEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
		String hdfsPath = StringHelper.getStringBeforeLastToken(sourceFileName, FORWARD_SLASH);

		logger.debug(getHandlerPhase(), "hdfsPath={}", hdfsPath);
		try {
			byte[] body = prepareBodyContents(hdfsPath);
			ActionEvent outputEvent = new ActionEvent();
			outputEvent.setHeaders(inputEvent.getHeaders());
			outputEvent.setBody(body);

			getHandlerContext().createSingleItemEventList(outputEvent);

			if (CollectionUtil.isNotEmpty(eventList)) {
				journal.setEventList(eventList);
				logger.debug(getHandlerPhase(), "_message=\"returning callback\" event_list_size={}", eventList.size());
				return Status.CALLBACK;
			}
		} catch (IOException e) {
			logger.warn(getHandlerPhase(), "_message=\"Exception occured\"", e);
			throw new HandlerException("could not get the directory listing from hdfs", e);
		}

		return Status.READY;
	}

	protected byte[] prepareBodyContents(final String directoryPath) throws IOException {
		WebHdfs webHdfs = null;
		try {
			if (webHdfs == null) {
				webHdfs = WebHdfsFactory.getWebHdfs(hostNames, port, hdfsUser, authOption);
			}
			final WebHdfsReader webHdfsReader = new WebHdfsReader();
			StringBuilder fileNamesStringBuilder = new StringBuilder();

			try {
				List<String> fileNames = webHdfsReader.list(webHdfs, directoryPath, false);

				if (CollectionUtil.isEmpty(fileNames)) {
					logger.warn(getHandlerPhase(), "_message=\"no files found\" directoryPath={}", directoryPath);
				}
				logger.info(getHandlerPhase(), "_message=\"found files\" directoryPath={} files_count={}",
						directoryPath, fileNames.size());
				for (final String fileName : fileNames) {
					fileNamesStringBuilder.append(fileName).append("\n");
				}
				logger.debug(getHandlerPhase(), fileNamesStringBuilder.toString());
			} catch (WebHdfsException e) {
				logger.info(getHandlerPhase(), "_message=\"path not found\" directoryPath={} error_message={}",
						directoryPath, e.getMessage());
			}
			return fileNamesStringBuilder.toString().getBytes();
		} finally {
			logger.debug(getHandlerPhase(), "releasing webhdfs connection");
			webHdfs.releaseConnection();
		}
	}

	private SimpleJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}
}