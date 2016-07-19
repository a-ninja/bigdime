package io.bigdime.handler.swift;

public enum SwiftWriterHandlerConstants {
	INSTANCE;
	public static SwiftWriterHandlerConstants getInstance() {
		return SwiftWriterHandlerConstants.INSTANCE;
	}

	public static final String USER_NAME = "user-name";
	public static final String PASSWORD = "password";
	public static final String AUTH_URL = "auth-url";
	public static final String TENANT_ID = "tenant-id";
	public static final String TENANT_NAME = "tenant-name";
	public static final String CONTAINER_NAME = "container-name";
	public static final String UPLOAD_OBJECT_TYPE = "upload-object-type"; // bytes
																			// or
																			// file
	public static final String INPUT_FILE_PATH_PATTERN = "input-file-path-pattern";
	public static final String OUTPUT_FILE_PATH_PATTERN = "output-file-path-pattern";

}
