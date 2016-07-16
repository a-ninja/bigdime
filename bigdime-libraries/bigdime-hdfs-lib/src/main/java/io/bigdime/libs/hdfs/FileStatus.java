package io.bigdime.libs.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

@JsonPropertyOrder({ "accessTime", "blockSize", "childrenNum", "fileId", "group", "length", "modificationTime", "owner",
		"pathSuffix", "permission", "replication", "type" })
public class FileStatus {

	@JsonProperty("accessTime")
	private Long accessTime;
	@JsonProperty("blockSize")
	private Long blockSize;
	@JsonProperty("childrenNum")
	private Long childrenNum;
	@JsonProperty("fileId")
	private Long fileId;
	@JsonProperty("group")
	private String group;
	@JsonProperty("length")
	private Long length;
	@JsonProperty("modificationTime")
	private Long modificationTime;
	@JsonProperty("owner")
	private String owner;
	@JsonProperty("pathSuffix")
	private String pathSuffix;
	@JsonProperty("permission")
	private String permission;
	@JsonProperty("replication")
	private Long replication;
	@JsonProperty("type")
	private String type;
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	/**
	 * 
	 * @return The accessTime
	 */
	@JsonProperty("accessTime")
	public Long getAccessTime() {
		return accessTime;
	}

	/**
	 * 
	 * @param accessTime
	 *            The accessTime
	 */
	@JsonProperty("accessTime")
	public void setAccessTime(Long accessTime) {
		this.accessTime = accessTime;
	}

	/**
	 * 
	 * @return The blockSize
	 */
	@JsonProperty("blockSize")
	public Long getBlockSize() {
		return blockSize;
	}

	/**
	 * 
	 * @param blockSize
	 *            The blockSize
	 */
	@JsonProperty("blockSize")
	public void setBlockSize(Long blockSize) {
		this.blockSize = blockSize;
	}

	/**
	 * 
	 * @return The childrenNum
	 */
	@JsonProperty("childrenNum")
	public Long getChildrenNum() {
		return childrenNum;
	}

	/**
	 * 
	 * @param childrenNum
	 *            The childrenNum
	 */
	@JsonProperty("childrenNum")
	public void setChildrenNum(Long childrenNum) {
		this.childrenNum = childrenNum;
	}

	/**
	 * 
	 * @return The fileId
	 */
	@JsonProperty("fileId")
	public Long getFileId() {
		return fileId;
	}

	/**
	 * 
	 * @param fileId
	 *            The fileId
	 */
	@JsonProperty("fileId")
	public void setFileId(Long fileId) {
		this.fileId = fileId;
	}

	/**
	 * 
	 * @return The group
	 */
	@JsonProperty("group")
	public String getGroup() {
		return group;
	}

	/**
	 * 
	 * @param group
	 *            The group
	 */
	@JsonProperty("group")
	public void setGroup(String group) {
		this.group = group;
	}

	/**
	 * 
	 * @return The length
	 */
	@JsonProperty("length")
	public Long getLength() {
		return length;
	}

	/**
	 * 
	 * @param length
	 *            The length
	 */
	@JsonProperty("length")
	public void setLength(Long length) {
		this.length = length;
	}

	/**
	 * 
	 * @return The modificationTime
	 */
	@JsonProperty("modificationTime")
	public Long getModificationTime() {
		return modificationTime;
	}

	/**
	 * 
	 * @param modificationTime
	 *            The modificationTime
	 */
	@JsonProperty("modificationTime")
	public void setModificationTime(Long modificationTime) {
		this.modificationTime = modificationTime;
	}

	/**
	 * 
	 * @return The owner
	 */
	@JsonProperty("owner")
	public String getOwner() {
		return owner;
	}

	/**
	 * 
	 * @param owner
	 *            The owner
	 */
	@JsonProperty("owner")
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * 
	 * @return The pathSuffix
	 */
	@JsonProperty("pathSuffix")
	public String getPathSuffix() {
		return pathSuffix;
	}

	/**
	 * 
	 * @param pathSuffix
	 *            The pathSuffix
	 */
	@JsonProperty("pathSuffix")
	public void setPathSuffix(String pathSuffix) {
		this.pathSuffix = pathSuffix;
	}

	/**
	 * 
	 * @return The permission
	 */
	@JsonProperty("permission")
	public String getPermission() {
		return permission;
	}

	/**
	 * 
	 * @param permission
	 *            The permission
	 */
	@JsonProperty("permission")
	public void setPermission(String permission) {
		this.permission = permission;
	}

	/**
	 * 
	 * @return The replication
	 */
	@JsonProperty("replication")
	public Long getReplication() {
		return replication;
	}

	/**
	 * 
	 * @param replication
	 *            The replication
	 */
	@JsonProperty("replication")
	public void setReplication(Long replication) {
		this.replication = replication;
	}

	/**
	 * 
	 * @return The type
	 */
	@JsonProperty("type")
	public String getType() {
		return type;
	}

	/**
	 * 
	 * @param type
	 *            The type
	 */
	@JsonProperty("type")
	public void setType(String type) {
		this.type = type;
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
