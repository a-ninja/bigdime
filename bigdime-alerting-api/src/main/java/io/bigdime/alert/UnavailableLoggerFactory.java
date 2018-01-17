/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import io.bigdime.alert.spi.AlertLoggerFactory;

public class UnavailableLoggerFactory implements AlertLoggerFactory {
    private static final UnavailableLoggerFactory instance = new UnavailableLoggerFactory();

    public static UnavailableLoggerFactory getInstance() {
        return instance;
    }

    @Override
    public Logger getLogger(String name) {
        return UnavailableLogger.instance;
    }

    @Override
    public Logger getLogger(Class<?> clazz) {
        return UnavailableLogger.instance;
    }

    private static class UnavailableLogger implements Logger {

        private static final UnavailableLogger instance = new UnavailableLogger();

        private UnavailableLogger() {
        }

        @Override
        public void debug(String source, String shortMessage, String message) {
        }

        @Override
        public void debug(String source, String shortMessage, String format, Object... o) {
        }

        @Override
        public void info(String source, String shortMessage, String message) {
        }

        @Override
        public void info(String source, String shortMessage, String format, Object... o) {
        }

        @Override
        public void warn(String source, String shortMessage, String message, Throwable t) {
        }

        @Override
        public void warn(String source, String shortMessage, String message) {
        }

        @Override
        public void warn(String source, String shortMessage, String format, Object... o) {
        }

        @Override
        public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
                          String message) {
        }

        @Override
        public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
                          String message, Throwable e) {
        }

        @Override
        public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, Throwable e,
                          String format, Object... o) {
        }

        @Override
        public void alert(AlertMessage alertMessage) {
        }

    }

}
