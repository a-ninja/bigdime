package io.bigdime.impl.biz.dao;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class Sink {

	private String name;
	private String description;
	private List<String> channeldesc;
	private List<Datahandler> datahandlers;
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
	@JsonProperty("channel-desc")
	public List<String> getChanneldesc() {
		return channeldesc;
	}
	public void setChanneldesc(List<String> channeldesc) {
		this.channeldesc = channeldesc;
	}
	@JsonProperty("data-handlers")
	public List<Datahandler> getDatahandlers() {
		return datahandlers;
	}
	public void setDatahandlers(List<Datahandler> datahandlers) {
		this.datahandlers = datahandlers;
	}
	
	
}
