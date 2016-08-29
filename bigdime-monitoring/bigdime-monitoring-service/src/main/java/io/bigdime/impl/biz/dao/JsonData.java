/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;



/**
 * A pojo encapsulating JSON template. 
 * @author Sandeep Reddy,Murthy
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class JsonData {

	private String name;
	private String type;
	private String cronexpression;
	private String autostart;
	private String namespace;
	private String description;
	private Source source; 
	private List<Sink> sink;
	private List<ChannelHandler> channel;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	@JsonProperty("cron-expression")
	public String getCronexpression() {
		return cronexpression;
	}
	@JsonProperty("cronexpression")
	public void setCronexpression(String cronexpression) {
		this.cronexpression = cronexpression;
	}
	@JsonProperty("auto-start")
	public String getAutostart() {
		return autostart;
	}
	public void setAutostart(String autostart) {
		this.autostart = autostart;
	}
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public Source getSource() {
		return source;
	}
	public void setSource(Source source) {
		this.source = source;
	}
	public List<Sink> getSink() {
		return sink;
	}
	public void setSink(List<Sink> sink) {
		this.sink = sink;
	}
	public List<ChannelHandler> getChannel() {
		return channel;
	}
	public void setChannel(List<ChannelHandler> channel) {
		this.channel = channel;
	}
	
	
}
