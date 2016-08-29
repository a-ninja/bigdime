/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.constants;

/**
 * @author jbrinnand, Neeraj Jain
 */
public final class WebHDFSWriterHandlerConstants {
	private static final WebHDFSWriterHandlerConstants instance = new WebHDFSWriterHandlerConstants();

	private WebHDFSWriterHandlerConstants() {
	}

	public static WebHDFSWriterHandlerConstants getInstance() {
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
	public static final String HDFS_OVERWRITE = "hdfsOverwrite";
	public static final String HDFS_PERMISSIONS = "hdfsPermissions";
	public static final String HIVE_PARTITION_NAMES = "hive_partition_names";
	public static final String HIVE_PARTITION_VALUES = "hive_partition_values";
	/**
	 * Allow user to specify whether to convert the whole hdfs path to lower or
	 * upper case by specifying "lower" or "upper". If this field is not
	 * specified, the path and partitions are left unchanged.
	 */
	public static final String HDFS_PATH_LOWER_UPPER_CASE = "hdfs-path-lower-upper-case";

}
