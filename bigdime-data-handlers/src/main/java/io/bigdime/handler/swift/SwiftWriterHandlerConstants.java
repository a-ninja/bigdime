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
    /**
     * Allows to query the RuntimeInfo based on this field. e.g. "handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:$1",
     */
    public static final String INPUT_DESCRIPTOR_PREFIX_PATTERN = "input-descriptor-prefix-pattern";

    /**
     * Pattern to get the fileName from the input-descriptor, e.g.
     * if the input-descriptor is handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:/webhdfs/v1/user/b_ndata/bigdime/2016-12-06/dw_lstg_gen/000499_0.gz,
     * then file-name-pattern would be .*\\/(\\[w_\\.]+)$ since the last token contains the file name.
     */
    public static final String FILE_NAME_FROM_DESCRIPTOR_PATTERN = "file-name-from-descriptor-pattern";

}