package io.bigdime.alert.impl.swift;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;

public class SwiftLogger implements Logger {
	private static final ConcurrentMap<String, SwiftLogger> loggerMap = new ConcurrentHashMap<>();
	private String swiftAlertLevel;

	public static final String EMPTYSTRING = "";
	private static final String APPLICATION_CONTEXT_PATH = "META-INF/application-context-monitoring.xml";
	public static final String SWIFT_USER_NAME_PROPERTY = "${swift.user.name}";
	public static final String SWIFT_PASSWORD_PROPERTY = "${swift.password}";
	public static final String SWIFT_AUTH_URL_PROPERTY = "${swift.auth.url}";
	public static final String SWIFT_TENANT_ID_PROPERTY = "${swift.tenant.id}";
	public static final String SWIFT_TENANT_NAME_PROPERTY = "${swift.tenant.name}";
	public static final String SWIFT_ALERT_CONTAINER_NAME_PROPERTY = "${swift.alert.container.name}";
	public static final String SWIFT_ALERT_LEVEL_PROPERTY = "${swift.alert.level}";
	public static final String SWIFT_BUFFER_SIZE_PROPERTY = "${swift.debugInfo.bufferSize}";

	public static final String IP_INIT_VAL = "10";
	private ExecutorService executorService;
	private Container container;
	private static String hostName = "UNKNOWN";
	private static String hostIp;
	long capacity = 10 * 1024;
	private boolean debugEnabled = false;
	private boolean infoEnabled = false;
	private boolean warnEnabled = false;

	final DateTimeFormatter logDtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
	static {
		try {
			hostName = InetAddress.getLocalHost().getHostName();
			Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
			while (e.hasMoreElements()) {
				NetworkInterface n = (NetworkInterface) e.nextElement();
				Enumeration<InetAddress> ee = n.getInetAddresses();
				while (ee.hasMoreElements()) {
					InetAddress i = (InetAddress) ee.nextElement();
					if (i.getHostAddress().startsWith(IP_INIT_VAL)) {
						hostIp = i.getHostAddress();
						break;
					}
				}
			}
		} catch (UnknownHostException e) {
			System.err.print("The host name is " + hostName);

		} catch (SocketException e1) {
			System.err.print("Error while connecting to " + hostName + " host");
		}
	}

	private SwiftLogger() {
	}

	public static SwiftLogger getLogger(String loggerName) {
		SwiftLogger logger = loggerMap.get(loggerName);
		if (logger == null) {
			logger = new SwiftLogger();
			loggerMap.put(loggerName, logger);
			ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(APPLICATION_CONTEXT_PATH);

			AccountConfig config = new AccountConfig();
			ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
			String containerName = context.getBeanFactory().resolveEmbeddedValue(SWIFT_ALERT_CONTAINER_NAME_PROPERTY);
			config.setUsername(context.getBeanFactory().resolveEmbeddedValue(SWIFT_USER_NAME_PROPERTY));
			config.setPassword(context.getBeanFactory().resolveEmbeddedValue(SWIFT_PASSWORD_PROPERTY));
			config.setAuthUrl(context.getBeanFactory().resolveEmbeddedValue(SWIFT_AUTH_URL_PROPERTY));
			config.setTenantId(context.getBeanFactory().resolveEmbeddedValue(SWIFT_TENANT_ID_PROPERTY));
			config.setTenantName(context.getBeanFactory().resolveEmbeddedValue(SWIFT_TENANT_NAME_PROPERTY));
			Account account = new AccountFactory(config).createAccount();
			logger.container = account.getContainer(containerName);
			logger.swiftAlertLevel = context.getBeanFactory().resolveEmbeddedValue(SWIFT_ALERT_LEVEL_PROPERTY);
			try {
				long bufferSize = Long.valueOf(beanFactory.resolveEmbeddedValue(SWIFT_BUFFER_SIZE_PROPERTY));
				logger.capacity = Long.valueOf(bufferSize);
				System.out.println("setting buffer size from property as:" + logger.capacity);
			} catch (Exception ex) {
				logger.capacity = 4 * 1024;
				System.out.println("setting default buffer size as:" + logger.capacity);
			}

			if (logger.swiftAlertLevel != null) {
				if (logger.swiftAlertLevel.equalsIgnoreCase("debug")) {
					setDebugEnabled(logger);
				} else if (logger.swiftAlertLevel.equalsIgnoreCase("info")) {
					setInfoEnabled(logger);
				} else if (logger.swiftAlertLevel.equalsIgnoreCase("warn")) {
					setWarnEnabled(logger);
				}
			}
			logger.executorService = Executors.newFixedThreadPool(1);
			System.out.println("swiftAlertContainerName=" + containerName + ", swiftAlertLevel="
					+ logger.swiftAlertLevel + ", capacity=" + logger.capacity);
			context.close();
		}
		return logger;
	}

	private boolean isDebugEnabled() {
		return debugEnabled;
	}

	private boolean isInfoEnabled() {
		return infoEnabled;
	}

	private boolean isWarnEnabled() {
		return warnEnabled;
	}

	@Override
	public void debug(String source, String shortMessage, String message) {
		if (isDebugEnabled()) {

			logDebugInfoToSwift(source, shortMessage, message, "debug");
		}
	}

	@Override
	public void debug(String source, String shortMessage, String format, Object... o) {
		if (isDebugEnabled()) {
			FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
			debug(source, shortMessage, ft.getMessage());
		}
	}

	@Override
	public void info(String source, String shortMessage, String message) {
		if (isInfoEnabled()) {
			logDebugInfoToSwift(source, shortMessage, message, "info");
		}
	}

	@Override
	public void info(String source, String shortMessage, String format, Object... o) {
		if (isInfoEnabled()) {
			FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
			info(source, shortMessage, ft.getMessage());
		}
	}

	@Override
	public void warn(String source, String shortMessage, String format, Object... o) {
		if (isWarnEnabled()) {
			FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
			warn(source, shortMessage, ft.getMessage());
		}
	}

	@Override
	public void warn(String source, String shortMessage, String message) {
		if (isWarnEnabled()) {
			warn(source, shortMessage, message, (Throwable) null);
		}
	}

	@Override
	public void warn(String source, String shortMessage, String message, Throwable t) {
		if (isWarnEnabled()) {
			logToSwift(source, shortMessage, message, "warn", t);
		}
	}

	@Override
	public void alert(final AlertMessage message) {
		alert(message.getAdaptorName(), message.getType(), message.getCause(), message.getSeverity(),
				message.getMessage());
	}

	@Override
	public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
			String message) {
		alert(source, alertType, alertCause, alertSeverity, message, (Throwable) null);
	}

	@Override
	public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
			String format, Object... o) {

		FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
		alert(source, alertType, alertCause, alertSeverity, ft.getMessage(), (Throwable) null);
	}

	@Override
	public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
			String message, Throwable t) {
		byte[] b = getExceptionByteArray(t);

		if (b != null) {
			message = message + " " + new String(b);
		}

		final StringBuilder sb = new StringBuilder();
		sb.append(
				"{} ERROR Thread={} adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\"")
				.append(" ").append(message).append("\n");
		final Object[] argArray = new Object[2 + 6];
		int i = 0;
		argArray[i++] = logDtf.print(System.currentTimeMillis());
		argArray[i++] = Thread.currentThread().getName();

		argArray[i++] = source;
		argArray[i++] = alertSeverity;
		argArray[i++] = "todo: set context";
		argArray[i++] = alertType.getMessageCode();
		argArray[i++] = alertType.getDescription();
		argArray[i++] = alertCause.getDescription();

		FormattingTuple ft = MessageFormatter.arrayFormat(sb.toString(), argArray);
		writeToSwift(source, ft.getMessage().getBytes());
	}

	private static ByteArrayOutputStream baos = new ByteArrayOutputStream();

	private byte[] getExceptionByteArray(Throwable t) {
		byte[] b = null;
		if (t != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			t.printStackTrace(ps);
			b = baos.toByteArray();
		}
		return b;
	}

	private void logToSwift(final String source, final String shortMessage, String message, final String level,
			Throwable t) {
		byte[] b = getExceptionByteArray(t);

		if (b != null) {
			message = message + " " + new String(b);
		}
		logDebugInfoToSwift(source, shortMessage, message, level);
	}

	private void logDebugInfoToSwift(final String source, final String shortMessage, final String message,
			final String level) {

		StringBuilder sb = new StringBuilder();
		byte[] dataTowrite = null;
		byte[] put = buildArgArray(level, sb, source, shortMessage, message, "").getBytes();
		synchronized (baos) {
			baos.write(put, 0, put.length);
			if (baos.size() >= capacity) {
				dataTowrite = baos.toByteArray();
				baos.reset();
			}
		}
		if (dataTowrite != null) {
			writeToSwift(source, dataTowrite);
		}
	}

	FutureTask<Object> futureTask = null;

	private void writeToSwift(final String source, byte[] dataTowrite) {
		SwiftLogTask logTask = new SwiftLogTask(container, source, dataTowrite);
		futureTask = new FutureTask<>(logTask);
		executorService.execute(futureTask);
		startHealthcheckThread();
	}

	private static void setDebugEnabled(SwiftLogger logger) {
		logger.debugEnabled = true;
		setInfoEnabled(logger);
	}

	private static void setInfoEnabled(SwiftLogger logger) {
		logger.infoEnabled = true;
		setWarnEnabled(logger);
	}

	private static void setWarnEnabled(SwiftLogger logger) {
		logger.warnEnabled = true;
	}

	private String buildArgArray(final String level, final StringBuilder sb, String source, String shortMessage,
			String format, Object... o) {
		sb.append("{} {} {} adaptor_name=\"{}\" message_context=\"{}\"").append(" ").append(format).append("\n");
		final Object[] argArray = new Object[3 + 2 + o.length];
		int j = 0;
		argArray[j++] = logDtf.print(System.currentTimeMillis());
		argArray[j++] = level;
		argArray[j++] = Thread.currentThread().getName();
		argArray[j++] = source;
		argArray[j++] = shortMessage;
		int i = 1;
		for (Object o1 : o) {
			i++;
			argArray[i] = o1;
		}
		FormattingTuple ft = MessageFormatter.arrayFormat(sb.toString(), argArray);
		return ft.getMessage();
	}

	protected void startHealthcheckThread() {
		new Thread() {
			@Override
			public void run() {
				try {
					System.out.print("heathcheck thread for swiftLogger");
					futureTask.get();
					System.out.print("heathcheck thread for swiftLogger, future task completed");
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		}.start();
	}
}
