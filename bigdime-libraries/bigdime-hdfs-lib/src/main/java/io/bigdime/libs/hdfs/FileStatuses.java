package io.bigdime.libs.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class FileStatuses {

    @JsonProperty("FileStatus")
    private List<FileStatus> fileStatus = new ArrayList<FileStatus>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * 
     * @return
     *     The fileStatus
     */
    @JsonProperty("FileStatus")
    public List<FileStatus> getFileStatus() {
        return fileStatus;
    }

    /**
     * 
     * @param fileStatus
     *     The FileStatus
     */
    @JsonProperty("FileStatus")
    public void setFileStatus(List<FileStatus> fileStatus) {
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
