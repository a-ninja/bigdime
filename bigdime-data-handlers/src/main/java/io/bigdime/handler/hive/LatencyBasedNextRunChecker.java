package io.bigdime.handler.hive;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;

public class LatencyBasedNextRunChecker extends AbstractNextRunChecker {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(LatencyBasedNextRunChecker.class));
	private long intervalInMillis = TimeUnit.DAYS.toMillis(1);

	@Override
	public long getDateTimeInMillisForNextRun(long lastRunDateTime, final HiveJdbcReaderHandlerConfig handlerConfig,
			Map<? extends String, ? extends Object> properties) {
		long now = getAdjustedCurrentTime(handlerConfig.getLatency());

		long nextRunDateTime = 0;

		if (lastRunDateTime == 0) {// this is the first time
			return getDateTimeInMillisForFirstRun(handlerConfig, now);
		} else {
			nextRunDateTime = getDateTimeInMillisForSubsequentRun(handlerConfig, now, lastRunDateTime);
		}
		return nextRunDateTime;
	}

	protected long getDateTimeInMillisForSubsequentRun(final HiveJdbcReaderHandlerConfig handlerConfig, long now,
			long lastRunDateTime) {
		long nextRunDateTime = 0;
		if (now - lastRunDateTime > handlerConfig.getMinGoBack()) {

			nextRunDateTime = lastRunDateTime + intervalInMillis;
			logger.info("getDateTimeInMillisForNextRun",
					"_message=\"time to set hiveConfDateTime.\" now={} lastRunDateTime={} hiveConfDateTime={} intervalInMillis={}",
					now, lastRunDateTime, nextRunDateTime, intervalInMillis);

		} else {
			logger.info("nothing to do", "now={} hiveConfDateTime={} intervalInMillis={} time_until_next_run={}", now,
					lastRunDateTime, intervalInMillis, handlerConfig.getMinGoBack() - (now - lastRunDateTime));
		}
		return nextRunDateTime;
	}

}
