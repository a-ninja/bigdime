/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.spi.AlertLoggerFactory;

/**
 * Default implementation of the alerting system, uses slf4j-log4j to log.
 * 
 * @author Neeraj Jain
 *
 */
public class AlertLoggerFactoryImpl implements AlertLoggerFactory {
	@Override
	public Logger getLogger(String name) {
		return MultipleLogger.getLogger(name);
	}

	@Override
	public Logger getLogger(Class<?> clazz) {
		return MultipleLogger.getLogger(clazz.getName());
	}

	private static final String APPLICATION_CONTEXT_PATH = "META-INF/application-context-monitoring.xml";

	private static final class MultipleLogger implements Logger {

		private static final ConcurrentMap<String, MultipleLogger> loggerMap = new ConcurrentHashMap<>();
		final List<Logger> loggers = new ArrayList<>();

		private MultipleLogger() {
		}

		public static Logger getLogger(String loggerName) {
			MultipleLogger logger = loggerMap.get(loggerName);
			if (logger == null) {
				logger = new MultipleLogger();
				loggerMap.put(loggerName, logger);

				try (final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
						APPLICATION_CONTEXT_PATH)) {
					logger.loggers.add(getDefaultLogger(loggerName));
					final String loggersProp = context.getBeanFactory().resolveEmbeddedValue("${loggers}");
					if (loggersProp.equalsIgnoreCase("${loggers}")) {
						System.out.println("no loggers configured, using default logger");
					} else {
						final String[] loggerArray = loggersProp.split(",");
						for (final String loggerClassName : loggerArray) {
							try {
								System.out.println("adding:" + loggerClassName);
								logger.loggers.add(getLoggerInstance(loggerClassName, loggerName));
							} catch (ClassNotFoundException | NoSuchMethodException | SecurityException
									| IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
								System.err.println("unable to get the instance of Logger for:" + loggerClassName
										+ ". ex=" + e.getMessage());
								e.printStackTrace(System.err);
							}
						}
					}
					context.close();
				}
			}
			return logger;
		}

		private static Logger getDefaultLogger(String loggerName) {
			return Slf4jLogger.getLogger(loggerName);
		}

		private static Logger getLoggerInstance(String loggerClassName, String loggerName)
				throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException {
			final Class<? extends Logger> c = Class.forName(loggerClassName).asSubclass(Logger.class);
			final Method method = c.getMethod("getLogger", String.class);
			return (Logger) method.invoke(null, loggerName);
		}

		@Override
		public void debug(String source, String shortMessage, String message) {
			for (Logger l : loggers) {
				l.debug(source, shortMessage, message);
			}
		}

		@Override
		public void debug(String source, String shortMessage, String format, Object... o) {
			for (Logger l : loggers) {
				l.debug(source, shortMessage, format, o);
			}
		}

		@Override
		public void info(String source, String shortMessage, String message) {
			for (Logger l : loggers) {
				l.info(source, shortMessage, message);
			}
		}

		@Override
		public void info(String source, String shortMessage, String format, Object... o) {
			for (Logger l : loggers) {
				l.info(source, shortMessage, format, o);
			}
		}

		@Override
		public void warn(String source, String shortMessage, String message) {
			for (Logger l : loggers) {
				l.warn(source, shortMessage, message);
			}
		}

		@Override
		public void warn(String source, String shortMessage, String format, Object... o) {
			for (Logger l : loggers) {
				l.warn(source, shortMessage, format, o);
			}
		}

		@Override
		public void warn(String source, String shortMessage, String message, Throwable t) {
			for (Logger l : loggers) {
				l.warn(source, shortMessage, message, t);
			}
		}

		@Override
		public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
				String message) {
			for (Logger l : loggers) {
				l.alert(source, alertType, alertCause, alertSeverity, message);
			}
		}

		@Override
		public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
				String message, Throwable t) {
			for (Logger l : loggers) {
				l.alert(source, alertType, alertCause, alertSeverity, message, t);
			}
		}

		@Override
		public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
				String format, Object... o) {
			for (Logger l : loggers) {
				l.alert(source, alertType, alertCause, alertSeverity, format, o);
			}
		}

		@Override
		public void alert(final AlertMessage message) {
			for (Logger l : loggers) {
				l.alert(message);
			}
		}
	}
}
