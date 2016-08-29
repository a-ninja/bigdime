/**
 * @author Sandeep Reddy,Murthy
 *
 */

package io.bigdime.impl.biz.dao;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class AdaptorConstants {
	
	private String environment;
	private List<Adaptor> adaptorList;
	public String getEnvironment() {
		return environment;
	}
	public void setEnvironment(String environment) {
		this.environment = environment;
	}
	@JsonProperty("adaptors")
	public List<Adaptor> getAdaptorList() {
		return adaptorList;
	}
	public void setAdaptorList(List<Adaptor> adaptorList) {
		this.adaptorList = adaptorList;
	}
	
	
	
}
