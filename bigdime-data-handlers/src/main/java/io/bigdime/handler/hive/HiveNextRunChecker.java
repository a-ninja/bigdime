package io.bigdime.handler.hive;

import java.util.Map;

public interface HiveNextRunChecker {

	/**
	 * If it's okay to run the next job, return the timestamp for the next job,
	 * return 0 otherwise.
	 * 
	 * @param lastRunDateTime
	 * @param handlerConfig
	 * @param properties
	 * @return time in millis, if it's the time to run the job, 0 otherwise
	 */
	public long getDateTimeInMillisForNextRun(long lastRunDateTime, final HiveJdbcReaderHandlerConfig handlerConfig,
			Map<? extends String, ? extends Object> properties);
}
