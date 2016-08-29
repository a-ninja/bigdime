package io.bigdime.impl.biz.dao;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class Datahandler {
	
	private String name;
	private String description;
	private String handlerclass;
	private Properties properties;
	private String channelclass;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	@JsonProperty("handler-class")
	public String getHandlerclass() {
		return handlerclass;
	}
	public void setHandlerclass(String handlerclass) {
		this.handlerclass = handlerclass;
	}
	public Properties getProperties() {
		return properties;
	}
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	@JsonProperty("channel-class")
	public String getChannelclass() {
		return channelclass;
	}
	public void setChannelclass(String channelclass) {
		this.channelclass = channelclass;
	}
	
	

}
