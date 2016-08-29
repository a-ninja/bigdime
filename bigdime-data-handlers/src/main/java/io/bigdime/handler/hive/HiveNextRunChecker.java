package io.bigdime.handler.hive;

import java.util.Map;

public interface HiveNextRunChecker {

	/**
	 * If it's okay to run the next job, return the timestamp for the next job,
	 * return 0 otherwise.
	 * 
	 * @return
	 */
	public long getDateTimeInMillisForNextRun(long lastRunDateTime, final HiveJdbcReaderHandlerConfig handlerConfig,
			Map<? extends String, ? extends Object> properties);
}
