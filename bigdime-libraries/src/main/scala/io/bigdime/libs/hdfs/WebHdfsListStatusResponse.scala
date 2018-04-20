package io.bigdime.libs.hdfs

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonProperty, JsonPropertyOrder}

/**
  * Created by neejain on 3/17/17.
  */
@JsonPropertyOrder(Array("FileStatuses"))
@JsonIgnoreProperties(ignoreUnknown = true)
class WebHdfsListStatusResponse(@JsonProperty("FileStatuses") val FileStatuses: FileStatuses = null, @JsonIgnore val additionalProperties: Map[String, Any])

@JsonIgnoreProperties(ignoreUnknown = true)
class FileStatuses(@JsonProperty("FileStatus") val FileStatus: List[FileStatus], @JsonIgnore val additionalProperties: Map[String, Any])