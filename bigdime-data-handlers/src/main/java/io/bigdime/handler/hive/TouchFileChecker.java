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

public class TouchFileChecker extends AbstractNextRunChecker {
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
			return getDateTimeInMillisForFirstRun(handlerConfig, now);
		} else {
			return getDateTimeInMillisForSubsequentRun(handlerConfig, now, lastRunDateTime, properties);
		}
	}

	private long getDateTimeInMillisForSubsequentRun(final HiveJdbcReaderHandlerConfig handlerConfig, long now,
			long lastRunDateTime, Map<? extends String, ? extends Object> properties) {
		long nextRunDateTime = lastRunDateTime + intervalInMillis;
		logger.info("getDateTimeInMillisForFirstRun",
				"_message=\"time to set hiveConfDateTime.\" now={} lastRunDateTime={} nextRunDateTime={} intervalInMillis={}",
				now, lastRunDateTime, nextRunDateTime, intervalInMillis);

		String tokenizedPath = handlerConfig.getTouchFile();

		final DateTimeFormatter yearDtf = DateTimeFormat.forPattern("yyyy");
		final DateTimeFormatter monthDtf = DateTimeFormat.forPattern("MM");
		final DateTimeFormatter dateDtf = DateTimeFormat.forPattern("dd");

		final Map<String, String> tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath,
				"\\$\\{([\\w\\-]+)\\}+");

		Map<String, String> localProperties = new HashMap<>();

		Set<String> tokenSet = tokenToTokenName.keySet();

		localProperties.put("yyyy", yearDtf.print(nextRunDateTime));
		localProperties.put("MM", monthDtf.print(nextRunDateTime));
		localProperties.put("dd", dateDtf.print(nextRunDateTime));

		String detokString = tokenizedPath;
		for (final String token : tokenSet) {
			String tokenName = tokenToTokenName.get(token);
			if (localProperties != null && localProperties.get(tokenName) != null)
				detokString = detokString.replace(token, localProperties.get(tokenName).toString());
			if (properties != null && properties.get(tokenName) != null)
				detokString = detokString.replace(token, properties.get(tokenName).toString());
		}
		try {
			if (webHdfsReader.getFileStatus(detokString) != null)
				return nextRunDateTime;
			else {
				logger.info("nothing to do",
						"now={} lastRunDateTime={} attempted_nextRunDateTime={} intervalInMillis={}", now,
						lastRunDateTime, nextRunDateTime, intervalInMillis);
				return 0;
			}
		} catch (IOException | WebHdfsException e) {
			logger.info("getDateTimeInMillisForNextRun", "file not found, failed with exception={}", e.getMessage());
			return 0;
		}
	}

}
