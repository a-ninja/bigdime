package io.bigdime.handler.webhdfs;

public class WebHDFSPathParserFactory {

	public static WebHDFSPathParser getWebHDFSPathParser(String hdfsPathFrom) {
		if (hdfsPathFrom.equals("headers")) {
			return new WebHDFSPathParserHeaderBased();
		} else if (hdfsPathFrom.equals("config")) {
			return new WebHDFSPathParserConfigBased();
		} else
			return null;
	}
}
