package io.bigdime.libs.hdfs;

public class WebHdfsException extends Exception {
	private static final long serialVersionUID = 1L;

	public WebHdfsException(String message) {
		super(message);
	}

	public WebHdfsException(String message, Exception e) {
		super(message, e);
	}

}
