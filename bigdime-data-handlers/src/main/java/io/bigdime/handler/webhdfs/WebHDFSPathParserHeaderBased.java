package io.bigdime.handler.webhdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.CollectionUtil;

public class WebHDFSPathParserHeaderBased implements WebHDFSPathParser {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(WebHDFSPathParserHeaderBased.class));

	public List<String> parse(String tokenizedPath, Map<? extends String, ? extends Object> properties,
			List<ActionEvent> eventList, String headerName) {
		logger.debug("getting hdfsPath from header", "headerName={}", headerName);

		if (CollectionUtil.isEmpty(eventList)) {
			logger.debug("getting hdfsPath from header: no events found", "eventList={}", eventList);
			return null;
		}
		List<String> availableHdfsDirectories = new ArrayList<>();
		for (final ActionEvent inputEvent : eventList) {
			availableHdfsDirectories.add(inputEvent.getHeaders().get(headerName));
		}
		return availableHdfsDirectories;
	}
}
