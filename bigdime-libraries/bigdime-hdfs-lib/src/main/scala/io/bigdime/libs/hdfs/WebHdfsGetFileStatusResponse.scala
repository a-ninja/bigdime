package io.bigdime.libs.hdfs

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonProperty, JsonPropertyOrder}

/**
  * Created by neejain on 3/17/17.
  */
@JsonPropertyOrder(Array("FileStatus"))
@JsonIgnoreProperties(ignoreUnknown = true)
case class WebHdfsGetFileStatusResponse(@JsonProperty("FileStatus") val FileStatus: FileStatus, @JsonIgnore val additionalProperties: Map[String, Any])

@JsonPropertyOrder(Array("accessTime", "blockSize", "childrenNum", "fileId", "group", "length", "modificationTime", "owner", "pathSuffix", "permission", "replication", "type"))
@JsonIgnoreProperties(ignoreUnknown = true)
case class FileStatus(@JsonProperty("accessTime") val accessTime: Long, @JsonProperty("blockSize") val blockSize: Long, @JsonProperty("childrenNum") val childrenNum: Long, @JsonProperty("fileId") val fileId: Long, @JsonProperty("group") val group: String, @JsonProperty("length") val length: Long, @JsonProperty("modificationTime") val modificationTime: Long, @JsonProperty("owner") val owner: String, @JsonProperty("pathSuffix") val pathSuffix: String, @JsonProperty("permission") val permission: String, @JsonProperty("replication") val replication: Long, @JsonProperty("type") val `type`: String, @JsonIgnore val additionalProperties: Map[String, Any])

