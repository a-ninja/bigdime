package io.bigdime.handler.hive;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;

public abstract class AbstractNextRunChecker implements HiveNextRunChecker {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(AbstractNextRunChecker.class));
	final private DateTimeZone dateTimeZone = DateTimeZone.forID("America/Los_Angeles");

	protected long getAdjustedCurrentTime() {
		final long systemTime = System.currentTimeMillis();
		final DateTime nowPacific = DateTime.now(dateTimeZone);
		long now = nowPacific.getMillis();

		logger.info("getAdjustedCurrentTime", "systemTime={} now={}", systemTime, now);
		return now;
	}

	protected long getAdjustedCurrentTime(final long latency) {
		final long systemTime = System.currentTimeMillis();
		final DateTime nowPacific = DateTime.now(dateTimeZone);
		long now = nowPacific.getMillis();
		now = now - latency;
		logger.info("getAdjustedCurrentTime", "systemTime={} latency={} now={}", systemTime, latency, now);
		return now;
	}

	protected long getDateTimeInMillisForFirstRun(final HiveJdbcReaderHandlerConfig handlerConfig, long now) {
		long nextRunDateTime = now - handlerConfig.getGoBackDays() * TimeUnit.DAYS.toMillis(1);
		logger.info("getDateTimeInMillisForFirstRun",
				"_message=\"first run, set hiveConfDateTime done. no need to check for the touchFile, it may not even be present.\" hiveConfDateTime={}",
				nextRunDateTime);
		return nextRunDateTime;
	}
}
