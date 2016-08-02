package io.bigdime.handler.webhdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.StringHelper;

public class ConfigBasedWebHDFSPathParser implements WebHDFSPathParser {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(ConfigBasedWebHDFSPathParser.class));

	public static void main(String[] args) {

		// 2016-08-01 16:32:18,207 DEBUG file-adaptor.WebHDFSReaderHandler
		// Thread=bigdime-ndata-source-1 - adaptor_name="bigdime-ndata-adaptor"
		// message_context="processing webhdfs-file-reader"
		// handler_id=5cc390d8-4ec2-45e4-a49c-a5c82c5cef58
		// next_index_to_read=104731 buffer_size=1048576 is_channel_open=true
		// current_file_path=/webhdfs/v1/apps/hdmi-set/sscience/dsbe/2016/07/24/categorydemandfinal/out-r-00078.gz
		// 2016-08-01 16:32:18,209 DEBUG
		// file-adaptor.SwiftAbstractByteWriterHandler Thread=sink for
		// bigdime-ndata-adaptor-1 - adaptor_name="bigdime-ndata-adaptor"
		// message_context="processing SwiftByteWriterHandler" _message="writing
		// to swift" swift_object_name=$1-$2-$3__$4/$5 object_length=3349884

		// Pattern p =
		// Pattern.compile("\\/\\w+\\/\\w+\\/\\w+\\/[\\w\\-]+\\/\\w+\\/\\w+\\/(\\w+)\\/(\\w+)\\/(\\w+)\\/(\\w+)\\/([\\w\\.\\-]+)$");
		//// Pattern p = Pattern.compile("(\\w+)");
		// Matcher m =
		// p.matcher("/webhdfs/v1/apps/hdmi-set/sscience/dsbe/2016/07/24/categorydemandfinal/out-r-00078.gz");

		Pattern p = Pattern.compile(
				"\\/\\w+\\/\\w+\\/\\w+\\/[\\w\\-]+\\/\\w+\\/\\w+\\/(\\w+)\\/(\\w+)\\/(\\w+)\\/(\\w+)\\/([\\w\\.\\-]+)$");
		// // Pattern p = Pattern.compile("(\\w+)");
		Matcher m = p.matcher("/webhdfs/v1/apps/hdmi-set/sscience/dsbe/2016/07/24/categorydemandfinal/out-r-00078.gz");
		//
		// while (m.find()) {
		// String token = m.group();// e.g. token=${account}
		// int count = m.groupCount();
		// for (int i = 0; i < count; i++) {
		// String headerName = m.group(i);// e.g.
		// System.out.println(token + " : " + headerName);
		// }
		//
		// // tokenToHeaderNameMap.put(token, headerName);
		// }
		Map<String, String> map = StringHelper.getTokenToTokenNameMap(
				"/webhdfs/v1/apps/hdmi-set/sscience/dsbe/${yyyy}/${MM}/${dd}/categorydemandfinal/");
		p = Pattern.compile("\\$\\{(\\w+)\\}+");
		m = p.matcher("/webhdfs/v1/apps/hdmi-set/sscience/dsbe/${yyyy}/${MM}/${dd}/categorydemandfinal/");
		System.out.println(map);
		// while (m.find()) {
		// String token = m.group();// e.g. token=${account}
		// System.out.println(token + " : " + m.group(1));
		//// int count = m.groupCount();
		//// for (int i = 0; i < count; i++) {
		//// String headerName = m.group(i);// e.g.
		//// System.out.println(token + " : " + headerName);
		//// }
		// }
		Map<String, Object> prop = new HashMap<>();
		prop.put("entity-name", "entity-name-val");
		List<String> str = new ConfigBasedWebHDFSPathParser()
				.parse("/webhdfs/v1/apps/hdmi-set/sscience/dsbe/${yyyy}/${MM}/${dd}/${entity-name}/", prop, null, null);
		System.out.println(str);
	}

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
	public List<String> parse(String tokenizedPath, Map<? extends String, ? extends Object> properties,
			List<ActionEvent> eventList, String headerName) {
		logger.debug("detokenizing string", "tokenizedPath={} properties={}", tokenizedPath, properties);
		final DateTimeFormatter yearDtf = DateTimeFormat.forPattern("yyyy");
		final DateTimeFormatter monthDtf = DateTimeFormat.forPattern("MM");
		final DateTimeFormatter dateDtf = DateTimeFormat.forPattern("dd");

		long time = System.currentTimeMillis();

		Map<String, String> localProperties = new HashMap<>();
		localProperties.put("yyyy", yearDtf.print(time));
		localProperties.put("MM", monthDtf.print(time));
		localProperties.put("dd", dateDtf.print(time));

		Map<String, String> tokenToTokenName = StringHelper.getTokenToTokenNameMap(tokenizedPath,
				"\\$\\{([\\w\\-]+)\\}+");
		System.out.println(tokenToTokenName);
		// {${dd}=dd, ${yyyy}=yyyy, ${MM}=MM}

		Set<String> tokenSet = tokenToTokenName.keySet();
		String detokString = tokenizedPath;

		// say token = ${yyyy}, tokenName will be yyyy.
		for (final String token : tokenSet) {
			String tokenName = tokenToTokenName.get(token);
			if (localProperties != null && localProperties.get(tokenName) != null)
				detokString = detokString.replace(token, localProperties.get(tokenName).toString());
			if (properties != null && properties.get(tokenName) != null)
				detokString = detokString.replace(token, properties.get(tokenName).toString());
		}

		logger.debug("detokenizing string: done", "tokenizedPath={} properties={} detokenizedString={}", tokenizedPath,
				properties, detokString);

		final List<String> hdfsPathList = new ArrayList<>();
		hdfsPathList.add(detokString);
		return hdfsPathList;
	}

}
