/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.impl;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.spi.AlertLoggerFactory;
import io.bigdime.core.commons.PropertyLoader;
import io.bigdime.core.commons.StringHelper;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation of the alerting system, uses slf4j-log4j to log.
 *
 * @author Neeraj Jain
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

    // private static final String APPLICATION_CONTEXT_PATH =
    // "META-INF/application-context-monitoring.xml";
    private static final String ENV_PROPERTIES = "env.properties";

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

                try {
                    Properties props = new PropertyLoader().loadEnvProperties(ENV_PROPERTIES);
                    logger.loggers.add(getDefaultLogger(loggerName));
                    final String loggersProp = props.getProperty("loggers");
                    if (StringHelper.isBlank(loggersProp)) {
                        System.out.println("no loggers configured, using default logger");
                    } else {
                        final String[] loggerArray = loggersProp.split(",");
                        for (final String loggerClassName : loggerArray) {
                            try {
                                System.out.println("adding:" + loggerClassName + ", for " + loggerName);
                                if (!loggerClassName.equals("io.bigdime.alert.impl.Slf4jLogger")) {
                                    logger.loggers.add(getLoggerInstance(loggerClassName, loggerName));
                                }
                            } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
                                    | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                                System.err.println("unable to get the instance of Logger for:" + loggerClassName
                                        + ". ex=" + e.getMessage());
                                e.printStackTrace(System.err);
                            }
                        }
                    }
                } catch (IOException e1) {
                    e1.printStackTrace(System.err);
                    throw new IllegalStateException("unable to read properties file :" + ENV_PROPERTIES + ":");
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
        public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, Throwable e,
                          String format, Object... o) {
            for (Logger l : loggers) {
                l.alert(source, alertType, alertCause, alertSeverity, e, format, o);
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
