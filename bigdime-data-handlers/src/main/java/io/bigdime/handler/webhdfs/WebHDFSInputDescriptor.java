package io.bigdime.handler.webhdfs;

import java.nio.channels.ReadableByteChannel;
import java.util.List;

import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.libs.hdfs.FileStatus;

class WebHDFSInputDescriptor implements InputDescriptor<String> {
	private static final String INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:";

	private String fullDescriptor;// complete filename, with prefix
	private String webhdfsPath;// complete filename

	private FileStatus currentFileStatus;
	private String currentFilePath;
	// private long fileLength = -1;
	private ReadableByteChannel fileChannel;

	@Override
	public String getNext(List<String> availableInputDescriptors, String lastInputDescriptor) {
		int indexOfLastInput = availableInputDescriptors.indexOf(lastInputDescriptor);
		if (availableInputDescriptors.size() > indexOfLastInput + 1) {
			return availableInputDescriptors.get(indexOfLastInput + 1);
		}
		return null;

	}

	@Override
	public void parseDescriptor(String inputDescriptor) {

		if (StringHelper.isBlank(inputDescriptor)) {
			throw new IllegalArgumentException("descriptor can't be null or empty");
		}

		if (inputDescriptor.indexOf(INPUT_DESCRIPTOR_PREFIX) < 0) {
			throw new IllegalArgumentException("descriptor must contain prefix:" + INPUT_DESCRIPTOR_PREFIX);

		}
		fullDescriptor = inputDescriptor;
		webhdfsPath = inputDescriptor.substring(INPUT_DESCRIPTOR_PREFIX.length());
		currentFilePath = webhdfsPath;
	}

	public String getWebhdfsPath() {
		return webhdfsPath;
	}

	public void setWebhdfsPath(String webhdfsPath) {
		this.webhdfsPath = webhdfsPath;
	}

	public String getFullDescriptor() {
		return fullDescriptor;
	}

	public String getDescriptorPrefix() {
		return INPUT_DESCRIPTOR_PREFIX;
	}

	public String createFullDescriptor(String webhdfsPath) {
		return INPUT_DESCRIPTOR_PREFIX + webhdfsPath;
	}

	public FileStatus getCurrentFileStatus() {
		return currentFileStatus;
	}

	public void setCurrentFileStatus(FileStatus currentFileStatus) {
		this.currentFileStatus = currentFileStatus;
	}

	public String getCurrentFilePath() {
		return currentFilePath;
	}

	public void setCurrentFilePath(String currentFilePath) {
		this.currentFilePath = currentFilePath;
	}

	public long getFileLength() {
		return getCurrentFileStatus().getLength();
	}

	public ReadableByteChannel getFileChannel() {
		return fileChannel;
	}

	public void setFileChannel(ReadableByteChannel fileChannel) {
		this.fileChannel = fileChannel;
	}
}
