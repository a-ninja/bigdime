/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;

public class AdaptorLogger extends AdaptorLoggerScala {
    private Logger logger;

    public AdaptorLogger(Logger _logger) {
        super(_logger);
        logger = logger();
    }

    public void debug(String shortMessage, String format, Object... o) {
        logger.debug(getAdaptorName(), shortMessage, format, o);
    }

    public void info(String shortMessage, String format, Object... o) {
        logger.info(getAdaptorName(), shortMessage, format, o);
    }

    public void warn(String shortMessage, String format, Object... o) {
        logger.warn(getAdaptorName(), shortMessage, format, o);
    }

    public void alert(ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, Throwable t, String format,
                      Object... o) {
        logger.alert(getAdaptorName(), alertType, alertCause, alertSeverity, t, format, o);
    }

    public void alert(ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, String format,
                      Object... o) {
        logger.alert(getAdaptorName(), alertType, alertCause, alertSeverity, null, format, o);
    }
}
