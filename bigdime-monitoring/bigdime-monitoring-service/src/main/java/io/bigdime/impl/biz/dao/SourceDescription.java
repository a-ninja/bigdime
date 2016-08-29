package io.bigdime.impl.biz.dao;

import java.util.Map;

import org.codehaus.jackson.map.annotate.JsonSerialize;
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class SourceDescription {
	
	Map<String,String> keyvalues;

	public Map<String, String> getKeyvalues() {
		return keyvalues;
	}

	public void setKeyvalues(Map<String, String> keyvalues) {
		this.keyvalues = keyvalues;
	}	

}
