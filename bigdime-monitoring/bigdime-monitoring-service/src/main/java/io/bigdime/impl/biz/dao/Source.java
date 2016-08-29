package io.bigdime.impl.biz.dao;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class Source {

	private String name;
	private String description;
	private String sourcetype;
	Map<String,String> srcdesc;
	List<Datahandler> datahandlers;
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
	@JsonProperty("source-type")
	public String getSourcetype() {
		return sourcetype;
	}
	public void setSourcetype(String sourcetype) {
		this.sourcetype = sourcetype;
	}
	@JsonProperty("src-desc")
	public Map<String, String> getSrcdesc() {
		return srcdesc;
	}
	public void setSrcdesc(Map<String, String> srcdesc) {
		this.srcdesc = srcdesc;
	}
	@JsonProperty("data-handlers")
	public List<Datahandler> getDatahandlers() {
		return datahandlers;
	}
	public void setDatahandlers(List<Datahandler> datahandlers) {
		this.datahandlers = datahandlers;
	}
		
}
