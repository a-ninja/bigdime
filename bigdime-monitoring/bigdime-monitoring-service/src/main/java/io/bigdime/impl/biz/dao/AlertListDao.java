/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;

import static io.bigdime.impl.biz.constants.ApplicationConstants.ALERT_NAME;
import static io.bigdime.impl.biz.constants.ApplicationConstants.LAST_N_DAYS;
import static io.bigdime.impl.biz.constants.ApplicationConstants.WRONGDATEFORMAT;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.alert.AlertException;
import io.bigdime.alert.AlertServiceRequest;
import io.bigdime.alert.AlertServiceResponse;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.ManagedAlert;
import io.bigdime.impl.biz.exception.AuthorizationException;
import io.bigdime.impl.biz.service.HbaseJsonDataService;
import io.bigdime.splunkalert.response.AlertBuilder;
import io.bigdime.splunkalert.retriever.SplunkSourceMetadataRetriever;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Offers methods that can be used by the implementation class to make calls for
 * alert data.
 * 
 * @author Sandeep Reddy,Murthy
 * @version 1.0
 */

@Component
public class AlertListDao {
	private static final Logger logger = LoggerFactory
			.getLogger(AlertListDao.class);

	@Value("${monitoring.numberofdays}")
	private String numberOfDays;
	
	@Autowired
	private AlertBuilder alertBuilder;
	@Autowired
	private SplunkSourceMetadataRetriever splunkSourceMetadataRetriever;
	@Autowired
	private MetadataStore metadataStore;
	@Autowired
	private HbaseJsonDataService hbaseJsonDataService;

	/**
	 * getAlerts method is used to make calls for alert data from the
	 * implementation class.
	 * 
	 * @return AlertData object which wraps the list of alert objects.
	 * @throws AlertException
	 * @throws MetadataAccessException
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws Exception
	 * @deprecated this method would be deprecated from the final cut
	 */
	public SplunkAlertData getSplunkAlerts() throws AlertException,
			MetadataAccessException {
		SplunkAlertData splunkAlertData = new SplunkAlertData();
		splunkAlertData.setRaisedAlerts(alertBuilder
				.getAlertsFromSplunk(ALERT_NAME));

		return splunkAlertData;
	}

	public AlertData getAlerts(String alertName,long start, int limit,String search) throws AlertException {
		AlertData alertData = new AlertData();
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		if (alertName != null && start !=WRONGDATEFORMAT && limit !=0) {		
			alertServiceRequest.setAlertId(alertName);
			Date currentTime = new Date();
			alertServiceRequest.setFromDate(new Date(start));
			alertServiceRequest.setLimit(limit);
			alertServiceRequest.setSearch(search);
			AlertServiceResponse<ManagedAlert> alertServiceResponse = splunkSourceMetadataRetriever
					.getAlerts(alertServiceRequest);
			alertData.setRaisedAlerts(alertServiceResponse.getAlerts());
			return alertData;
		}else
		{
			throw new AuthorizationException(
					"The parameters provided in the call are  invalid,insufficient or not properly parsed");
		}
	}

	public AlertData getAlerts(String alertName, long fromDate, long toDate)
			throws AlertException {
		if (alertName != null && fromDate != WRONGDATEFORMAT
				&& toDate != WRONGDATEFORMAT) {
			AlertData alertData = new AlertData();
			AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
			alertServiceRequest.setAlertId(alertName);
			alertServiceRequest.setFromDate(new Date(fromDate));
			alertServiceRequest.setToDate(new Date(toDate));
			AlertServiceResponse<ManagedAlert> alertServiceResponse = splunkSourceMetadataRetriever
					.getAlerts(alertServiceRequest);
			alertData.setRaisedAlerts(alertServiceResponse.getAlerts());

			return alertData;
		} else {
			throw new AuthorizationException(
					"The parameters provided in the call are invalid, insufficient or not properly parsed");
		}
	}
	
	public ArrayNode getSetOfAlerts() throws MetadataAccessException {
		Set<String> datasourceSet = metadataStore.getDataSources();
		ObjectMapper om = new ObjectMapper();
		ArrayNode datasourcesJson = om.createArrayNode();
		if (!datasourceSet.isEmpty()) {	
			for (String datasource : datasourceSet) {
				ObjectNode sourceNode = om.createObjectNode();
				sourceNode.put("label", datasource);
				datasourcesJson.add(sourceNode);
			}
			return datasourcesJson;
		} else {
			throw new AuthorizationException(
					"No applications found for monitoring");
		}
	}
	
	 public List<Long> getDates(String alertName,long start) throws AlertException{
	   AlertServiceRequest alertServiceRequest= new AlertServiceRequest();
	   alertServiceRequest.setAlertId(alertName);
	   alertServiceRequest.setFromDate(new Date(start));
	   List<Long> list=splunkSourceMetadataRetriever.getDates(alertServiceRequest);
	   return list;
   }
	
	public JsonData getJSON(String templateId){
		if(templateId !=null){		
		   return hbaseJsonDataService.getJSON(templateId);
		}else {
			throw new AuthorizationException(
					"The parameters provided in the call are invalid, insufficient or not properly parsed");
		}
	}
	
	public Datahandler getHandler(String handlerId){
		if(handlerId !=null){		
		   return hbaseJsonDataService.getHandler(handlerId);
		}else {
			throw new AuthorizationException(
					"The parameters provided in the call are invalid, insufficient or not properly parsed");
		}
	}
	
	public  List<AdaptorConstants> getAdaptorConstants(){
		return hbaseJsonDataService.getAdaptorConstants();
	}

}
