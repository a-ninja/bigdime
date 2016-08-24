package io.bigdime.libs.hdfs;

public class WebHdfsException extends Exception {
	private static final long serialVersionUID = 1L;

	private int statusCode;

	public WebHdfsException(int _statusCode, String message) {
		super(message);
		this.statusCode = _statusCode;
	}

	public WebHdfsException(String message, Exception e) {
		super(message, e);
	}

	public int getStatusCode() {
		return statusCode;
	}
}
