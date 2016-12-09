package io.bigdime.handler.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.libs.hdfs.WebHdfsException;
import io.bigdime.libs.hdfs.WebHdfsReader;

class TouchFileChecker extends AbstractNextRunChecker {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(TouchFileChecker.class));
	private long intervalInMillis = TimeUnit.DAYS.toMillis(1);

	private WebHdfsReader webHdfsReader;

	public TouchFileChecker(final WebHdfsReader _webHdfsReader) {
		webHdfsReader = _webHdfsReader;
	}

	@Override
	public long getDateTimeInMillisForNextRun(long lastRunDateTime, final HiveJdbcReaderHandlerConfig handlerConfig,
			Map<? extends String, ? extends Object> properties) {

		long now = getAdjustedCurrentTime();

		if (lastRunDateTime == 0) {// this is the first time
			return getDateTimeInMillisForFirstRun(handlerConfig, now, properties);
		} else {
			return getDateTimeInMillisForSubsequentRun(handlerConfig, now, lastRunDateTime, properties);
		}
	}

	/*
	 * Set the nextRunDateTime to currentTime - goBackDays. Say this time is T1.
	 * If any folder is found for T1 or after, return the time as T1.
	 */
	protected long getDateTimeInMillisForFirstRun(final HiveJdbcReaderHandlerConfig handlerConfig, long now,
			Map<? extends String, ? extends Object> properties) {
		final long nextRunDateTime = now - handlerConfig.getGoBackDays() * TimeUnit.DAYS.toMillis(1);
		logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run.\" attempted nextRunDateTime={}",
				nextRunDateTime);
		// return getDateTimeInMillis(handlerConfig, now, 0, nextRunDateTime,
		// properties);
		long time = nextRunDateTime;
		long tempNextRunDateTime = nextRunDateTime;
		do {

			time = getDateTimeInMillis(handlerConfig, now, 0, tempNextRunDateTime, properties);
			logger.info("getDateTimeInMillisForFirstRun", "_message=\"first run.\" tempNextRunDateTime={} output={}",
					tempNextRunDateTime, time);
			if (time == 0) {
				tempNextRunDateTime = tempNextRunDateTime + intervalInMillis;
			} else {
				break;
			}
			// break if time is a positive value,or next_time is greater than
			// now
		} while (time == 0 && tempNextRunDateTime < now);
		logger.info("getDateTimeInMillisForFirstRun", "_message=\"returning value\" nextRunDateTime={}",
				nextRunDateTime);
		return nextRunDateTime;
	}

	/*
	 * Set the nextRunDateTime to currentTime - goBackDays. Say this time is T1.
	 * If any folder is found for T1 or after, return the time as T1.
	 */

	private long getDateTimeInMillisForSubsequentRun(final HiveJdbcReaderHandlerConfig handlerConfig, long now,
			long lastRunDateTime, Map<? extends String, ? extends Object> properties) {

		final long nextRunDateTime = lastRunDateTime + intervalInMillis;
		long time = nextRunDateTime;
		long tempNextRunDateTime = nextRunDateTime;
		do {

			time = getDateTimeInMillis(handlerConfig, now, lastRunDateTime, tempNextRunDateTime, properties);
			logger.info("getDateTimeInMillisForSubsequentRun",
					"_message=\"subsequent run.\" tempNextRunDateTime={} output={}", tempNextRunDateTime, time);
			if (time == 0) {
				tempNextRunDateTime = tempNextRunDateTime + intervalInMillis;
			} else {
				break;
			}
		} while (time == 0 && tempNextRunDateTime < now);
		if (time == 0)
			return time;
		else
			return nextRunDateTime;
	}

	private long getTouchFileDate(long nextRunDateTime) {
		return nextRunDateTime + intervalInMillis;
	}

	private long getDateTimeInMillis(final HiveJdbcReaderHandlerConfig handlerConfig, long now, long lastRunDateTime,
			final long nextRunDateTime, Map<? extends String, ? extends Object> properties) {
		logger.info("getDateTimeInMillis",
				"_message=\"will check touchfile.\" now={} nextRunDateTime={} intervalInMillis={}", now,
				nextRunDateTime, intervalInMillis);

		String tokenizedPath = handlerConfig.getTouchFile();

		final DateTimeFormatter yearDtf = DateTimeFormat.forPattern("yyyy");
		final DateTimeFormatter monthDtf = DateTimeFormat.forPattern("MM");
		final DateTimeFormatter dateDtf = DateTimeFormat.forPattern("dd");

		final Map<String, String> tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath,
				"\\$\\{([\\w\\-]+)\\}+");

		Map<String, String> localProperties = new HashMap<>();

		Set<String> tokenSet = tokenToTokenName.keySet();

		// if we are loading data for update_date>12/20, the touch file has the
		// date of 12/21.
		// so, we need to add the interval twice.
		long touchFileDate = getTouchFileDate(nextRunDateTime);// this adds a
																// day to the
																// attempted
																// date
		localProperties.put("yyyy", yearDtf.print(touchFileDate));
		localProperties.put("MM", monthDtf.print(touchFileDate));
		localProperties.put("dd", dateDtf.print(touchFileDate));

		String detokString = tokenizedPath;
		for (final String token : tokenSet) {
			String tokenName = tokenToTokenName.get(token);
			if (localProperties != null && localProperties.get(tokenName) != null)
				detokString = detokString.replace(token, localProperties.get(tokenName).toString());
			if (properties != null && properties.get(tokenName) != null)
				detokString = detokString.replace(token, properties.get(tokenName).toString());
		}
		try {
			logger.info("checking touchfile", "file_to_check={}", detokString);
			if (webHdfsReader.getFileStatus(detokString, 2) != null) {
				logger.info("found touchfile", "file_to_check={}", detokString);
				return nextRunDateTime;
			} else {
				logger.info("nothing to do, touchfile not found",
						"now={} lastRunDateTime={} attempted_nextRunDateTime={} intervalInMillis={} file_to_check={}",
						now, lastRunDateTime, nextRunDateTime, intervalInMillis, detokString);
				return 0;
			}
		} catch (IOException | WebHdfsException e) {
			logger.info("getDateTimeInMillisForNextRun", "file not found, failed with exception={}", e.getMessage());
			return 0;
		}
	}

}
