package io.bigdime.handler.swift;

public enum SwiftWriterHandlerConstants {
	INSTANCE;
	public static SwiftWriterHandlerConstants getInstance() {
		return SwiftWriterHandlerConstants.INSTANCE;
	}

	/**
	 * Username to connect to swift
	 */
	public static final String USER_NAME = "user-name";

	/**
	 * Password to connect to swift
	 */
	public static final String PASSWORD = "password";

	/**
	 * authUrl to connect to swift
	 */
	public static final String AUTH_URL = "auth-url";

	/**
	 * tenant id in which the container lives
	 */
	public static final String TENANT_ID = "tenant-id";

	/**
	 * Tenant name corresponding to tenantId
	 */
	public static final String TENANT_NAME = "tenant-name";

	/**
	 * Name of the container
	 */
	public static final String CONTAINER_NAME = "container-name";

	/**
	 * bytes or file
	 */
	public static final String UPLOAD_OBJECT_TYPE = "upload-object-type";

	/**
	 * e.g. webhdfs file path
	 */
	public static final String INPUT_FILE_PATH_PATTERN = "input-file-path-pattern";

	/**
	 * swift file name pattern, e.g. 20160101__entityName/
	 */
	public static final String OUTPUT_FILE_PATH_PATTERN = "output-file-path-pattern";

	/**
	 * swift file prefix, e.g. directory name
	 */
	public static final String FILE_PATH_PREFIX_PATTERN = "file-path-prefix-pattern";

	/**
	 * swift file name pattern, including prefix and filename, e.g.
	 * 20160101__entityName/file_1.txt
	 */
	public static final String FILE_PATH_PATTERN = "file-path-pattern";
}
