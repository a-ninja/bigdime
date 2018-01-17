package io.bigdime.libs.hdfs

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonProperty, JsonPropertyOrder}

/**
  * Created by neejain on 3/16/17.
  */
@JsonPropertyOrder(Array("FileChecksum"))
@JsonIgnoreProperties(ignoreUnknown = true)
case class WebHdfsGetFileChecksumResponse(@JsonProperty("FileChecksum") val FileChecksum: HdfsFileChecksum, @JsonIgnore val additionalProperties: Map[String, Any])

@JsonPropertyOrder(Array("algorithm", "bytes", "length"))
@JsonIgnoreProperties(ignoreUnknown = true)
class HdfsFileChecksum(@JsonProperty("algorithm") val algorithm: String, @JsonProperty("bytes") val bytes: String, @JsonProperty("length") val length: Int, @JsonIgnore val additionalProperties: Map[String, AnyRef])

