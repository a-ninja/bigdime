package io.bigdime.libs.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonPropertyOrder({ "FileStatuses" })
public class WebHdfsListStatusResponse {

	@JsonProperty("FileStatuses")
	private FileStatuses fileStatuses;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The fileStatuses
	 */
	@JsonProperty("FileStatuses")
	public FileStatuses getFileStatuses() {
		return fileStatuses;
	}

	/**
	 * 
	 * @param fileStatuses
	 *            The FileStatuses
	 */
	@JsonProperty("FileStatuses")
	public void setFileStatuses(FileStatuses fileStatuses) {
		this.fileStatuses = fileStatuses;
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
