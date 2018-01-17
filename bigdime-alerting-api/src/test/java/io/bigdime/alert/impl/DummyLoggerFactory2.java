/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.impl;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.spi.AlertLoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DummyLoggerFactory2 implements AlertLoggerFactory {
	@Override
	public Logger getLogger(String name) {
		return StaticSlf4jLogger.getLogger(name);
	}

	@Override
	public Logger getLogger(Class<?> clazz) {
		return StaticSlf4jLogger.getLogger(clazz.getName());
	}

	private static final class StaticSlf4jLogger implements Logger {

		private static final ConcurrentMap<String, StaticSlf4jLogger> loggerMap = new ConcurrentHashMap<>();
		private org.slf4j.Logger slf4jLogger = null;

		private StaticSlf4jLogger() {
		}

		public static Logger getLogger(String loggerName) {
			StaticSlf4jLogger logger = loggerMap.get(loggerName);
			if (logger == null) {
				logger = new StaticSlf4jLogger();
				loggerMap.put(loggerName, logger);
				logger.slf4jLogger = org.slf4j.LoggerFactory.getLogger(loggerName);
			}
			return logger;
		}

		@Override
		public void debug(String source, String shortMessage, String message) {
			slf4jLogger.debug("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage,
					message);
		}

		@Override
		public void debug(String source, String shortMessage, String format, Object... o) {
			if (slf4jLogger.isDebugEnabled()) {
				final StringBuilder sb = new StringBuilder();
				final Object[] argArray = buildArgArray(sb, source, shortMessage, format, o);
				slf4jLogger.debug(sb.toString(), argArray);
			}
		}

		@Override
		public void info(String source, String shortMessage, String message) {
			slf4jLogger.info("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage,
					message);
		}

		@Override
		public void info(String source, String shortMessage, String format, Object... o) {
			if (slf4jLogger.isInfoEnabled()) {
				final StringBuilder sb = new StringBuilder();
				final Object[] argArray = buildArgArray(sb, source, shortMessage, format, o);
				slf4jLogger.info(sb.toString(), argArray);
			}
		}

		@Override
		public void warn(String source, String shortMessage, String message) {
			slf4jLogger.warn("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage,
					message);
		}

		@Override
		public void warn(String source, String shortMessage, String format, Object... o) {
			if (slf4jLogger.isWarnEnabled()) {
				final StringBuilder sb = new StringBuilder();
				final Object[] argArray = buildArgArray(sb, source, shortMessage, format, o);
				slf4jLogger.warn(sb.toString(), argArray);
			}
		}

		@Override
		public void warn(String source, String shortMessage, String message, Throwable t) {
			slf4jLogger.warn("adaptor_name={} message_context=\"{}\" detail_message=\"{}\"", source, shortMessage,
					message, t);
		}

		@Override
		public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
				String message) {
			slf4jLogger.error(
					"adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\" detail_message=\"{}\"",
					source, alertSeverity, "TODO:set context", alertType.getMessageCode(), alertType.getDescription(),
					alertCause.getDescription(), message);
		}

		@Override
		public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
				String message, Throwable t) {
			slf4jLogger.error(
					"adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\" detail_message=\"{}\"",
					source, alertSeverity, "TODO:set context", alertType.getMessageCode(), alertType.getDescription(),
					alertCause.getDescription(), message, t);
		}

		@Override
		public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, Throwable e,
				String format, Object... o) {
			final StringBuilder sb = new StringBuilder();
			sb.append(
					"adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\"")
					.append(" ").append(format);
			final Object[] argArray = new Object[6 + o.length];
			argArray[0] = source;
			argArray[1] = alertSeverity;
			argArray[2] = "todo: set context";
			argArray[3] = alertType.getMessageCode();
			argArray[4] = alertType.getDescription();
			argArray[5] = alertCause.getDescription();

			int i = 5;
			for (Object o1 : o) {
				i++;
				argArray[i] = o1;
			}
			slf4jLogger.error(sb.toString(), argArray);
		}

		@Override
		public void alert(final AlertMessage message) {
			alert(message.getAdaptorName(), message.getType(), message.getCause(), message.getSeverity(),
					message.getMessage());
		}

		private Object[] buildArgArray(final StringBuilder sb, String source, String shortMessage, String format,
				Object... o) {
			sb.append("adaptor_name=\"{}\" message_context=\"{}\"").append(" ").append(format);
			final Object[] argArray = new Object[2 + o.length];
			argArray[0] = source;
			argArray[1] = shortMessage;
			int i = 1;
			for (Object o1 : o) {
				i++;
				argArray[i] = o1;
			}
			return argArray;
		}

	}

}
