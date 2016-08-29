/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert.retriever;

import io.bigdime.alert.Alert;
import io.bigdime.alert.AlertException;
import io.bigdime.alert.AlertServiceRequest;
import io.bigdime.alert.AlertServiceResponse;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.ManagedAlert;
import io.bigdime.alert.ManagedAlertService;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;
import io.bigdime.splunkalert.common.exception.AuthorizationException;
import io.bigdime.splunkalert.handler.SplunkAuthTokenProvider;
import io.bigdime.splunkalert.util.HttpClientProvider;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.httpclient.HttpStatus;

import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SCHEME;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.COUNT_VALUE;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.COUNT_KEY;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.JSON_OUTPUT_MODE;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.OUTPUT_MODE_KEY;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.AUTHORIZATION_HEADER_KEY;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.AUTHORIZATION_HEADER_VALUE_PREFIX;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.PATH;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SOURCE_NAME;

/**
 * Provides methods to fetch metadata from splunk and returns it to the caller.
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */

@Component
public class SplunkSourceMetadataRetriever {
	private static final Logger logger = LoggerFactory
			.getLogger(SplunkSourceMetadataRetriever.class);

	// String path;
	@Autowired
	private SplunkAuthTokenProvider splunkAuthTokenProvider;
	@Value("${splunk.host}")
	private String SPLUNK_HOST;
	@Value("${splunk.port}")
	private int SPLUNK_PORT;
	@Autowired
	private HttpClientProvider httpClientProvider;
	@Autowired
	private ManagedAlertService managedAlertService;

	/**
	 * Makes call to Splunk to 1)gets Authorization key and then 2)Uses the auth
	 * key to make a call to splunk for getting the metadata and 3)Uses the
	 * metadata to make a call to splunk for getting the actual alert data.
	 * 
	 * @return JsonNode object that contains metadata/data of the alerts
	 * @throws IOException
	 */
	public JsonNode getSourceMetadata(String authToken, Properties properties)
			throws AuthorizationException, URISyntaxException, IOException {

		String path = properties.getProperty(PATH);
		ObjectMapper om = new ObjectMapper();
		JsonNode sourceMetadata = null;
		CloseableHttpResponse response = null;
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme(SCHEME).setHost(SPLUNK_HOST).setPort(SPLUNK_PORT)
				.setParameter(COUNT_KEY, COUNT_VALUE)
				.addParameter(OUTPUT_MODE_KEY, JSON_OUTPUT_MODE).setPath(path);

		try {
			response = getResponse(uriBuilder, authToken);
			String authenticationToken = null;
			sourceMetadata = om.readTree(response.getEntity().getContent());
			response.close();
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
				logger.info(SOURCE_NAME, "Authentication Phase",
						"Authentication code failed. Getting a new code");

				authenticationToken = splunkAuthTokenProvider.getNewAuthToken();
				response = getResponse(uriBuilder, authenticationToken);
				sourceMetadata = om.readTree(response.getEntity().getContent());
				if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
					throw new AuthorizationException(
							"Unable to retrieve splunk metadata. Response was:"
									+ response.getStatusLine().toString());
				}
			}
		} finally {
			if (response != null) {
				response.close();
			}
		}
		return sourceMetadata;
	}

	/**
	 * Method to make a call to Splunk with specific URI and authorization token
	 * 
	 * @param uriBuilder
	 *            :Builds splunk URI that would be called,it could be a metadata
	 *            /data retrevial URI
	 * @param authToken
	 *            :Authorization token that would be used to make an
	 *            authorization with splunk.This would be a return type from
	 *            SplunkTokenProvider getauthToken call.
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws ClientProtocolException
	 */

	private CloseableHttpResponse getResponse(URIBuilder uriBuilder,
			String authToken) throws URISyntaxException,
			ClientProtocolException, IOException {
		HttpUriRequest request = null;
		request = new HttpGet(uriBuilder.build());
		request.addHeader(AUTHORIZATION_HEADER_KEY,
				AUTHORIZATION_HEADER_VALUE_PREFIX + authToken);
		CloseableHttpClient httpClient = httpClientProvider.getHttpClient();
		CloseableHttpResponse response = httpClient.execute(request);
		return response;
	}

	public AlertServiceResponse<ManagedAlert> getAlerts(
			AlertServiceRequest alertServiceRequest) throws AlertException {
		try {
			return managedAlertService.getAlerts(alertServiceRequest);
		} catch (AlertException e) {
			logger.info("HBASE", "FETCH Alerts from HBASE",
					"Unable to fetch alerts from HBASE :" + e.getMessage());
			throw e;
		}

	}

	/**
	 * Checks if the data is already present in the persistent store.
	 * 
	 * @param alertMessage
	 * @param alertStatus
	 * @param comment
	 */
	public void updateAlert(Alert alertMessage, ALERT_STATUS alertStatus,
			String comment) {
		try {
			managedAlertService.updateAlert(alertMessage, alertStatus, comment);
		} catch (AlertException e) {
			logger.info("HBASE", "Insert alert in HBASE",
					"Failed to insert/update into HBASE " + e.getMessage());
		}

	}
	
	/**
	 * 
	 * @param alertServiceRequest
	 * @return list of dates in long format representing the offsets for the given alertServiceRequest criteria
	 * @throws AlertException
	 */
	
	public List<Long> getDates(AlertServiceRequest alertServiceRequest) throws AlertException{
		try {
			return managedAlertService.getDates(alertServiceRequest);
		} catch (AlertException e) {
			logger.info("HBASE", "FETCH Dates from HBASE",
					"Unable to fetch Dates from HBASE :" + e.getMessage());
			throw e;
		}
	} 

}
