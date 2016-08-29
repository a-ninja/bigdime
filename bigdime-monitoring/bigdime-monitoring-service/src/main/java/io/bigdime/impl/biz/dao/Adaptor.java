/**
 * @author Sandeep Reddy,Murthy
 *
 */

package io.bigdime.impl.biz.dao;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)public class Adaptor {
	
	private String name;
	private List<String> handlerList; 
	private List<String> handlerClassList;
	private List<String> sinkList;
	private List<String> sinkClassList;
	private List<String> channelList;
	private List<String> nonDefaults;
	private List<String> mandatoryFields;
	private List<String> nonessentialFields;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@JsonProperty("handlerlist")
	public List<String> getHandlerList() {
		return handlerList;
	}
	public void setHandlerList(List<String> handlerList) {
		this.handlerList = handlerList;
	}
	@JsonProperty("handlerclasslist")
	public List<String> getHandlerClassList() {
		return handlerClassList;
	}
	public void setHandlerClassList(List<String> handlerClassList) {
		this.handlerClassList = handlerClassList;
	}
	@JsonProperty("sinkhandlerlist")
	public List<String> getSinkList() {
		return sinkList;
	}
	public void setSinkList(List<String> sinkList) {
		this.sinkList = sinkList;
	}
	@JsonProperty("sinkhandlerclasslist")
	public List<String> getSinkClassList() {
		return sinkClassList;
	}
	public void setSinkClassList(List<String> sinkClassList) {
		this.sinkClassList = sinkClassList;
	}
	@JsonProperty("channellist")
	public List<String> getChannelList() {
		return channelList;
	}
	public void setChannelList(List<String> channelList) {
		this.channelList = channelList;
	}
	@JsonProperty("nondefaults")
	public List<String> getNonDefaults() {
		return nonDefaults;
	}
	public void setNonDefaults(List<String> nonDefaults) {
		this.nonDefaults = nonDefaults;
	}
	@JsonProperty("mandatoryfields")
	public List<String> getMandatoryFields() {
		return mandatoryFields;
	}
	public void setMandatoryFields(List<String> mandatoryFields) {
		this.mandatoryFields = mandatoryFields;
	}
	@JsonProperty("nonessentialfields")
	public List<String> getNonessentialFields() {
		return nonessentialFields;
	}
	public void setNonessentialFields(List<String> nonessentialFields) {
		this.nonessentialFields = nonessentialFields;
	}
	
	
}
