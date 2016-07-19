/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.webhdfs;

/**
 * @author Neeraj Jain
 */
public final class WebHDFSReaderHandlerConstants {
	private static final WebHDFSReaderHandlerConstants instance = new WebHDFSReaderHandlerConstants();

	private WebHDFSReaderHandlerConstants() {
	}

	public static WebHDFSReaderHandlerConstants getInstance() {
		return instance;
	}

	public static final String BATCH_SIZE = "batchSize";
	public static final String HOST_NAMES = "hostNames";
	public static final String PORT = "port";
	public static final String HDFS_FILE_NAME = "hdfsFileName";
	public static final String HDFS_FILE_NAME_PREFIX = "hdfsFileNamePrefix";
	public static final String HDFS_FILE_NAME_EXTENSION = "hdfsFileNameExtension";

	public static final String HDFS_PATH = "hdfsPath";
	public static final String HDFS_USER = "hdfsUser";
	public static final String READ_HDFS_PATH_FROM = "readHdfsPathFrom";
	/**
	 * Allow user to specify whether to convert the whole hdfs path to lower or
	 * upper case by specifying "lower" or "upper". If this field is not
	 * specified, the path and partitions are left unchanged.
	 */
	public static final String HDFS_PATH_LOWER_UPPER_CASE = "hdfs-path-lower-upper-case";

}
