package io.bigdime.handler.webhdfs;

import io.bigdime.core.commons.StringHelper;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;

public class WebHDFSReaderHandlerConfig {
	public enum READ_HDFS_PATH_FROM {
		CONFIG, HEADERS;

		public static READ_HDFS_PATH_FROM getByValue(String val) {
			if (StringHelper.isBlank(val))
				return null;
			for (final READ_HDFS_PATH_FROM pathFrom : READ_HDFS_PATH_FROM.values()) {
				if (StringHelper.equalsIgnoreCaseAndTrimmed(val, pathFrom.name()))
					return pathFrom;
			}
			return null;
		}
	};

	private String hostNames;
	private int port;

	/**
	 * Besides absolute path, the path with patterns are supported.
	 * /path1/path2/path3/${yyyy}/${MM}/${dd}/${entityName}/
	 * /path1/path2/path3/${yyyy}-${MM}-${dd}/${entityName}/
	 * /path1/path2/path3/path4?date=${yyyy}-${MM}-${dd}
	 * 
	 * 
	 * 
	 * 
	 */
	private String hdfsPath;
	private String hdfsUser;
	private String entityName;
	private HDFS_AUTH_OPTION authOption;
	private int bufferSize;
	private READ_HDFS_PATH_FROM readHdfsPathFrom; // CONFIG | HEADERS
	private String waitForFileName;
	private final String INPUT_DESCRIPTOR_PREFIX = "/webhdfs/v1/";
	private final String PATH_INPUT_DESCRIPTOR_PREFIX = "/webhdfs/v1/";

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

	public String getHdfsUser() {
		return hdfsUser;
	}

	public void setHdfsUser(String hdfsUser) {
		this.hdfsUser = hdfsUser;
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

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public READ_HDFS_PATH_FROM getReadHdfsPathFrom() {
		return readHdfsPathFrom;
	}

	public void setReadHdfsPathFrom(String readHdfsPathFrom) {
		this.readHdfsPathFrom = READ_HDFS_PATH_FROM.getByValue(readHdfsPathFrom);
	}

	public void setReadHdfsPathFrom(READ_HDFS_PATH_FROM readHdfsPathFrom) {
		this.readHdfsPathFrom = readHdfsPathFrom;
	}

	public String getINPUT_DESCRIPTOR_PREFIX() {
		return INPUT_DESCRIPTOR_PREFIX;
	}

	public String getPATH_INPUT_DESCRIPTOR_PREFIX() {
		return PATH_INPUT_DESCRIPTOR_PREFIX;
	}

	public String getWaitForFileName() {
		return waitForFileName;
	}

	public void setWaitForFileName(String waitForFileName) {
		this.waitForFileName = waitForFileName;
	}

}
