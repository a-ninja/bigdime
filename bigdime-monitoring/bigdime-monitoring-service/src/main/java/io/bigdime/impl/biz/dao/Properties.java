package io.bigdime.impl.biz.dao;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class Properties {
	private String brokers;
	private String offsetdatadir;
	private String messageSize;
	private String batchSize;
	private String printstats;
	private String basepath;
	private String buffersize;
	private String preservebasepath;
	private String preserverelativepath;
	private String hostNames;
	private String port;
	private String hdfsFileNamePrefix;
	private String hdfsFileNameExtension;
	private String hdfsPath;
	private String hdfsUser;
	private String hdfsOverwrite;
	private String hdfsPermissions;
	private String regex;
	private String partitionnames;
	private String headername;
	private String schemaFileName;
	private String entityName;
	private String validationtype;
	private String channelclass;
	private String datepartitionname;
	private String datepartitioninputformat;
	private String datepartitionoutputformat;
	private String partitionnamesoutputorder;
	private String hdfspathloweruppercase;
	private String printstatsdurationinseconds;
	private String channelcapacity;
	private String hivemetastoreuris;
	
	
	@JsonProperty("hive.metastore.uris")
	public String getHivemetastoreuris() {
		return hivemetastoreuris;
	}
	public void setHivemetastoreuris(String hivemetastoreuris) {
		this.hivemetastoreuris = hivemetastoreuris;
	}
	@JsonProperty("print-stats-duration-in-seconds")
	public String getPrintstatsdurationinseconds() {
		return printstatsdurationinseconds;
	}
	public void setPrintstatsdurationinseconds(String printstatsdurationinseconds) {
		this.printstatsdurationinseconds = printstatsdurationinseconds;
	}
	@JsonProperty("channel-capacity")
	public String getChannelcapacity() {
		return channelcapacity;
	}
	public void setChannelcapacity(String channelcapacity) {
		this.channelcapacity = channelcapacity;
	}
	@JsonProperty("hdfs-path-lower-upper-case")
	public String getHdfspathloweruppercase() {
		return hdfspathloweruppercase;
	}
	public void setHdfspathloweruppercase(String hdfspathloweruppercase) {
		this.hdfspathloweruppercase = hdfspathloweruppercase;
	}
	@JsonProperty("partition-names-output-order")
	public String getPartitionnamesoutputorder() {
		return partitionnamesoutputorder;
	}
	public void setPartitionnamesoutputorder(String partitionnamesoutputorder) {
		this.partitionnamesoutputorder = partitionnamesoutputorder;
	}
	public String getBrokers() {
		return brokers;
	}
	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}	
	@JsonProperty("offset-data-dir")
	public String getOffsetdatadir() {
		return offsetdatadir;
	}
	public void setOffsetdatadir(String offsetdatadir) {
		this.offsetdatadir = offsetdatadir;
	}
	public String getMessageSize() {
		return messageSize;
	}
	public void setMessageSize(String messageSize) {
		this.messageSize = messageSize;
	}
	@JsonProperty("print-stats")
	public String getPrintstats() {
		return printstats;
	}
	public void setPrintstats(String printstats) {
		this.printstats = printstats;
	}
	@JsonProperty("base-path")
	public String getBasepath() {
		return basepath;
	}
	public void setBasepath(String basepath) {
		this.basepath = basepath;
	}
	@JsonProperty("buffer-size")
	public String getBuffersize() {
		return buffersize;
	}
	public void setBuffersize(String buffersize) {
		this.buffersize = buffersize;
	}
	@JsonProperty("preserve-base-path")
	public String getPreservebasepath() {
		return preservebasepath;
	}
	public void setPreservebasepath(String preservebasepath) {
		this.preservebasepath = preservebasepath;
	}
	@JsonProperty("preserve-relative-path")
	public String getPreserverelativepath() {
		return preserverelativepath;
	}
	public void setPreserverelativepath(String preserverelativepath) {
		this.preserverelativepath = preserverelativepath;
	}
	public String getHostNames() {
		return hostNames;
	}
	public void setHostNames(String hostNames) {
		this.hostNames = hostNames;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(String batchSize) {
		this.batchSize = batchSize;
	}
	public String getHdfsFileNamePrefix() {
		return hdfsFileNamePrefix;
	}
	public void setHdfsFileNamePrefix(String hdfsFileNamePrefix) {
		this.hdfsFileNamePrefix = hdfsFileNamePrefix;
	}
	public String getHdfsFileNameExtension() {
		return hdfsFileNameExtension;
	}
	public void setHdfsFileNameExtension(String hdfsFileNameExtension) {
		this.hdfsFileNameExtension = hdfsFileNameExtension;
	}
	public String getHdfsPath() {
		return hdfsPath;
	}
	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}
	public String getHdfsUser() {
		return hdfsUser;
	}
	public void setHdfsUser(String hdfsUser) {
		this.hdfsUser = hdfsUser;
	}
	public String getHdfsOverwrite() {
		return hdfsOverwrite;
	}
	public void setHdfsOverwrite(String hdfsOverwrite) {
		this.hdfsOverwrite = hdfsOverwrite;
	}
	public String getHdfsPermissions() {
		return hdfsPermissions;
	}
	public void setHdfsPermissions(String hdfsPermissions) {
		this.hdfsPermissions = hdfsPermissions;
	}
	public String getRegex() {
		return regex;
	}
	public void setRegex(String regex) {
		this.regex = regex;
	}
	@JsonProperty("partition-names")
	public String getPartitionnames() {
		return partitionnames;
	}
	public void setPartitionnames(String partitionnames) {
		this.partitionnames = partitionnames;
	}
	@JsonProperty("header-name")
	public String getHeadername() {
		return headername;
	}
	public void setHeadername(String headername) {
		this.headername = headername;
	}
	public String getSchemaFileName() {
		return schemaFileName;
	}
	public void setSchemaFileName(String schemaFileName) {
		this.schemaFileName = schemaFileName;
	}
	
	public String getEntityName() {
		return entityName;
	}
	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}
	@JsonProperty("validation-type")
	public String getValidationtype() {
		return validationtype;
	}
	public void setValidationtype(String validationtype) {
		this.validationtype = validationtype;
	}
	public String getChannelclass() {
		return channelclass;
	}
	public void setChannelclass(String channelclass) {
		this.channelclass = channelclass;
	}
	@JsonProperty("date-partition-name")
	public String getDatepartitionname() {
		return datepartitionname;
	}
	public void setDatepartitionname(String datepartitionname) {
		this.datepartitionname = datepartitionname;
	}
	@JsonProperty("date-partition-input-format")
	public String getDatepartitioninputformat() {
		return datepartitioninputformat;
	}
	public void setDatepartitioninputformat(String datepartitioninputformat) {
		this.datepartitioninputformat = datepartitioninputformat;
	}
	@JsonProperty("date-partition-output-format")
	public String getDatepartitionoutputformat() {
		return datepartitionoutputformat;
	}
	public void setDatepartitionoutputformat(String datepartitionoutputformat) {
		this.datepartitionoutputformat = datepartitionoutputformat;
	}
	
	
}
