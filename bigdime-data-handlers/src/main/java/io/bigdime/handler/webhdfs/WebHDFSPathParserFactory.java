package io.bigdime.handler.webhdfs;

public class WebHDFSPathParserFactory {

	public static WebHDFSPathParser getWebHDFSPathParser(String hdfsPathFrom) {
		if (hdfsPathFrom.equals("headers")) {
			return new HeaderBasedWebHDFSPathParser();
		} else if (hdfsPathFrom.equals("config")) {
			return new ConfigBasedWebHDFSPathParser();
		} else
			return null;
	}
}
