/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

/**
 * Adaptors and other framework classes should use this interface to log the
 * alert messages. For any alert, it's preferred to have following fields:
 * <p>
 * time, e.g. "2015-08-01 10:11:12:123456" data adaptor name, e.g.
 * "tracking-data" ERROR message_context, e.g. "data reading phase" alert
 * severity, e.g. BLOCKER alert code number, e.g. "BIG-0001" alert name, e.g.
 * "ingestion failed" alert cause, e.g. "data validation failed" detail message,
 * e.g. "data validation failed, input checksum = #01234567, output checksum =
 * #76543210"
 * <p>
 * For other log messages, it's preferred to have following fields:
 * <p>
 * time, e.g. "2015-08-01 10:11:12:123456" data adaptor name, e.g.
 * "tracking-data" log level, e.g. DEBUG message_context, e.g.
 * "data reading phase" detail message, e.g.
 * "read 1048576 bytes from /data/adaptor/files/file001.csv"
 * <p>
 * <p>
 * Sample alert message: adaptor_name="tracking-data" LEVEL=ERROR
 * message_context="data reading phase" alert_severity="BLOCKER"
 * alert_code="BIG-0001" alert_name="ingestion failed" alert_cause=
 * "data validation failed" detail_message="data validation failed, input
 * checksum = #01234567, output checksum = #76543210"
 *
 * @author Neeraj Jain
 */
public interface Logger {

    /**
     * Log a message at DEBUG level.
     *
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param message      detailed message such as "read 1048576 bytes from
     *                     /data/adaptor/files/file001.csv"
     */
    public void debug(String source, String shortMessage, String message);

    /**
     * Log a message at DEBUG level. first two arguments are formatted by the
     * library. Provide the format argument and other parameters to print more
     * fields.
     *
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param format       the format string
     * @param o            argument to supply value in format string
     */
    public void debug(String source, String shortMessage, String format, Object... o);

    /**
     * Log a message at INFO level.
     *
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param message      detailed message such as "read 1048576 bytes from
     *                     /data/adaptor/files/file001.csv
     */
    public void info(String source, String shortMessage, String message);

    /**
     * Log a message at WARN level.
     *
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param format       the format string
     * @param o            argument to supply value in format string
     */
    public void info(String source, String shortMessage, String format, Object... o);

    /**
     * Log a message at WARN level with stack trace.
     *
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param message      detailed message such as "read 1048576 bytes from
     *                     /data/adaptor/files/file001.csv
     * @param t            object containing exception information
     */
    public void warn(String source, String shortMessage, String message, Throwable t);

    /**
     * Log a message at WARN level with simple message.
     *
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param message      detailed message such as "read 1048576 bytes from
     *                     /data/adaptor/files/file001.csv
     */
    public void warn(String source, String shortMessage, String message);

    /**
     * @param source       name of the data adaptor
     * @param shortMessage short message such as "data reading phase"
     * @param format       the format string
     * @param o            argument to supply value in format string
     */
    public void warn(String source, String shortMessage, String format, Object... o);

    /**
     * Log a message at ERROR level with specified source, ALERT_TYPE,
     * ALERT_CAUSE, message and ALERT_SEVERITY.
     *
     * @param source        name of the adaptor that caused the alert
     * @param alertType     alert message type
     * @param alertCause    reason for alert
     * @param message       detailed alert message
     * @param alertSeverity severity that indicated impact on end user
     */
    public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
                      String message);

    /**
     * Log a message at ERROR level with specified source, ALERT_TYPE,
     * ALERT_CAUSE, message, ALERT_SEVERITY and stack trace.
     *
     * @param source        name of the adaptor that caused the alert
     * @param alertType     alert message type
     * @param alertCause    reason for alert
     * @param message       detailed alert message
     * @param alertSeverity severity that indicated impact on end user
     * @param e             the exception (throwable) to log
     */
    public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
                      String message, Throwable e);

    /**
     * @param source        name of the adaptor that caused the alert
     * @param alertType     alert message type
     * @param alertCause    reason for alert
     * @param alertSeverity severity that indicated impact on end user
     * @param format        the format string
     * @param o             argument to supply value in format string
     */
    public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, Throwable e,
                      String format, Object... o);

    /**
     * Log a message at ERROR level with values from alertMessage.
     *
     * @param alertMessage
     */
    public void alert(AlertMessage alertMessage);

    /**
     * ALERT_TYPE provides the available set of alert types that provides
     * information about impact to end users.
     *
     * @author Neeraj Jain
     */
    public enum ALERT_TYPE {
        /**
         *
         */
        ADAPTOR_FAILED_TO_START("BIG-0001", "adaptor failed to start"),

        /**
         * Failed to ingest data due to some error. Adaptor could still continue
         * to run to ingest next set of data or adaptor could stop if certain
         * threshold has been matched.
         */
        INGESTION_FAILED("BIG-0002", "ingestion failed"),

        /**
         * Adapter can look for the last run time and log a message with this
         * type if it finds that the last cycle was not run.
         */
        INGESTION_DID_NOT_RUN("BIG-0003", "ingestion did not run"),

        /**
         * If the adaptor decides to stop the ingestion due to any issues it's
         * experiencing.
         */
        INGESTION_STOPPED("BIG-0004", "ingestion stopped"),

        /**
         * Use this message type if the adaptor discovers that the input schema
         * has changed.
         */
        DATA_FORMAT_CHANGED("BIG-0005", "data format has been changed"),

        /**
         *
         */
        FILE_ARCHIVE_FAILED("BIG-0006", "unable to archive the source file"),
        /**
         * Unknown error
         */
        OTHER_ERROR("BIG-9999", "uncategorized");

        private String messageCode;
        private String description;

        private ALERT_TYPE(String messageCode, String description) {
            this.messageCode = messageCode;
            this.description = description;
        }

        public String getMessageCode() {
            return messageCode;
        }

        public String getDescription() {
            return description;
        }

    }

    /**
     * ALERT_CAUSE provides high level information about why this alert was
     * raised.
     *
     * @author Neeraj Jain
     */
    public enum ALERT_CAUSE {
        /**
         * adaptor's configuration is not valid.
         */
        INVALID_ADAPTOR_CONFIGURATION("adaptor configuration is invalid"),

        /**
         * Validation error occurred.
         */
        VALIDATION_ERROR("data validation error"),

        /**
         * Data could not be retrieved because the message size was too big.
         */
        MESSAGE_TOO_BIG("input message too big"),

        /**
         * Data contains unsupported character type.
         */
        UNSUPPORTED_DATA_TYPE("unsupported data or character type"),

        /**
         * schema from the input side has changed. If the adaptor is tolerant to
         * changes in schema, log a DATA_FORMAT_CHANGED otherwise log a
         * INGESTION_FAILED or INGESTION_STOPPED message.
         */
        INPUT_DATA_SCHEMA_CHANGED("input data schema changed"),

        /**
         * Unable to read data from source.
         */
        INPUT_ERROR("data could not be read from source"),

        /**
         * data for the next scheduled batch not available.
         */
        MISSING_DATA("data not available"),

        /**
         * application internal error.
         */
        APPLICATION_INTERNAL_ERROR("internal error"),

        /**
         * ingestion stopped.
         */
        SHUTDOWN_COMMAND("shutdown command received");

        private String description;

        private ALERT_CAUSE(String cause) {
            this.description = cause;
        }

        public String getDescription() {
            return description;
        }

    }

    /**
     * ALERT_SEVERITY indicates the severity of impact on the end users.
     *
     * @author Neeraj Jain
     */
    public enum ALERT_SEVERITY {
        /**
         * Indicates critical impact on operations. e.g. if a data adaptor fails
         * to ingest data, it might lead to delay in generating analytics
         * reports.
         */
        BLOCKER, /**
         * Alert message of this type must be taken care of at the
         * earliest since it could impact end user. e.g. if a data
         * adaptor didn't ingest the data because the data did not
         * arrive. Although it might not indicate an issue in the
         * adaptor, it needs to be addressed.
         */
        MAJOR, /**
         * Indicates some impact on operations but the problem can be
         * circumvented.
         */
        NORMAL;
    }

}
