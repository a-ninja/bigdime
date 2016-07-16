package io.bigdime.libs.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonPropertyOrder({ "FileStatus" })
public class WebHdfsGetFileStatusResponse {

	@JsonProperty("FileStatus")
	private FileStatus fileStatus;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The fileStatus
	 */
	@JsonProperty("FileStatus")
	public FileStatus getFileStatus() {
		return fileStatus;
	}

	/**
	 * 
	 * @param fileStatus
	 *            The FileStatus
	 */
	@JsonProperty("FileStatus")
	public void setFileStatus(FileStatus fileStatus) {
		this.fileStatus = fileStatus;
	}

	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

}
