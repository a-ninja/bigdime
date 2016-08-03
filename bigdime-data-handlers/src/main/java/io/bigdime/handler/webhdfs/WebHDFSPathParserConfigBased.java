package io.bigdime.handler.webhdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.StringHelper;

public class WebHDFSPathParserConfigBased implements WebHDFSPathParser {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(WebHDFSPathParserConfigBased.class));

	/**
	 * Detokenizes the tokenizedPath; e.g.
	 * 
	 * If path is : /path1/path2/path3/${yyyy}/${MM}/${dd}/${entityName}/, then
	 * this method replaces ${yyyy}, ${MM}, and ${dd} with current date elements
	 * and returns the final path.
	 * 
	 * If any of the token is found in the properties, they are picked from
	 * there. If not, current new date object is generated.
	 * 
	 * 
	 * @param tokenizedPath
	 * @param properties
	 * @return
	 */
	private static final int DEFAULT_GO_BACK_DAYS = 5;

	public List<String> parse(String tokenizedPath, Map<? extends String, ? extends Object> properties,
			List<ActionEvent> eventList, String headerName) {
		logger.debug("detokenizing string", "tokenizedPath={} properties={}", tokenizedPath, properties);
		final DateTimeFormatter yearDtf = DateTimeFormat.forPattern("yyyy");
		final DateTimeFormatter monthDtf = DateTimeFormat.forPattern("MM");
		final DateTimeFormatter dateDtf = DateTimeFormat.forPattern("dd");
		final List<String> hdfsPathList = new ArrayList<>();
		int goBackDays = PropertyHelper.getIntProperty(properties, WebHDFSReaderHandlerConstants.BATCH_SIZE,
				DEFAULT_GO_BACK_DAYS);

		long currentTime = System.currentTimeMillis();

		long oldTime = currentTime - TimeUnit.DAYS.toMillis(goBackDays);

		Map<String, String> tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath,
				"\\$\\{([\\w\\-]+)\\}+");
		// System.out.println(tokenToTokenName);
		// {${dd}=dd, ${yyyy}=yyyy, ${MM}=MM}

		Map<String, String> localProperties = new HashMap<>();

		Set<String> tokenSet = tokenToTokenName.keySet();

		for (int i = 0; i < goBackDays; i++) {
			long time = oldTime + TimeUnit.DAYS.toMillis(i);
			logger.debug("detokenizing string:", "currentTime={} time={} tokenizedPath={}", currentTime, time,
					tokenizedPath);

			localProperties.put("yyyy", yearDtf.print(time));
			localProperties.put("MM", monthDtf.print(time));
			localProperties.put("dd", dateDtf.print(time));
			String detokString = tokenizedPath;

			// say token = ${yyyy}, tokenName will be yyyy.
			for (final String token : tokenSet) {
				String tokenName = tokenToTokenName.get(token);
				if (localProperties != null && localProperties.get(tokenName) != null)
					detokString = detokString.replace(token, localProperties.get(tokenName).toString());
				if (properties != null && properties.get(tokenName) != null)
					detokString = detokString.replace(token, properties.get(tokenName).toString());
			}

			logger.debug("detokenizing string: done", "tokenizedPath={} properties={} detokenizedString={}",
					tokenizedPath, properties, detokString);

			hdfsPathList.add(detokString);

		}
		return hdfsPathList;
	}

}
