package io.bigdime.handler.webhdfs;

import java.util.List;
import java.util.Map;

import io.bigdime.core.ActionEvent;

public interface WebHDFSPathParser {
//	public List<String> parse(String tokenizedPath, Map<? extends String, ? extends Object> properties);
	public List<String> parse(String tokenizedPath, Map<? extends String, ? extends Object> properties, List<ActionEvent> eventList, String headerName);

}
