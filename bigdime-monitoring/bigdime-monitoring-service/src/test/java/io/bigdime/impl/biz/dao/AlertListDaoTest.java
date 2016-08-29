/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.ENVIRONMENT_VALUE;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.SOURCE_TYPE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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
import io.bigdime.impl.biz.dao.AlertData;
import io.bigdime.impl.biz.dao.AlertListDao;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.JsonData;
import io.bigdime.impl.biz.exception.AuthorizationException;
import io.bigdime.impl.biz.service.HbaseJsonDataService;
import io.bigdime.splunkalert.SplunkAlert;
import io.bigdime.splunkalert.response.AlertBuilder;
import io.bigdime.splunkalert.retriever.SplunkSourceMetadataRetriever;

import org.codehaus.jackson.node.ArrayNode;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * 
 * @author samurthy
 * 
 */
public class AlertListDaoTest {
	private static final Logger logger = LoggerFactory
			.getLogger(AlertListDaoTest.class);

	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE, "Test Phase", "Setting the environment");
		System.setProperty(ENVIORNMENT, ENVIRONMENT_VALUE);

	}

	@Test
	public void getSplunkAlertsTest() throws AlertException,
			MetadataAccessException {
		AlertListDao alertListDao = new AlertListDao();
		AlertBuilder alertBuilder = Mockito.mock(AlertBuilder.class);
		List<SplunkAlert> list = new ArrayList<SplunkAlert>();
		Mockito.when(
				alertBuilder.getAlertsFromSplunk(Mockito.any(String.class)))
				.thenReturn(list);
		ReflectionTestUtils
				.setField(alertListDao, "alertBuilder", alertBuilder);
		Assert.assertNotNull(alertListDao.getSplunkAlerts());
	}

	@Test(expectedExceptions = AuthorizationException.class)
	public void getSplunkAlertsExceptionTest() throws AlertException,
			MetadataAccessException {
		AlertListDao alertListDao = new AlertListDao();
		AlertBuilder alertBuilder = Mockito.mock(AlertBuilder.class);
		Mockito.when(
				alertBuilder.getAlertsFromSplunk(Mockito.any(String.class)))
				.thenThrow(new AuthorizationException("testException"));
		AlertData alertData = Mockito.mock(AlertData.class);
		Mockito.doNothing().when(alertData).setRaisedAlerts(Mockito.anyList());
		ReflectionTestUtils
				.setField(alertListDao, "alertBuilder", alertBuilder);
		alertListDao.getSplunkAlerts();
	}

	@Test
	public void getAlertsTest() throws AlertException {
		AlertListDao alertListDao = new AlertListDao();
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		AlertServiceRequest alertServiceRequest = Mockito
				.mock(AlertServiceRequest.class);
		AlertServiceResponse alertServiceResponse = Mockito
				.mock(AlertServiceResponse.class);
		Mockito.when(
				splunkSourceMetadataRetriever
						.getAlerts((AlertServiceRequest) Mockito.any()))
				.thenReturn(alertServiceResponse);
		Mockito.when(alertServiceResponse.getAlerts()).thenReturn(
				new ArrayList<ManagedAlert>());
		ReflectionTestUtils.setField(alertListDao,
				"splunkSourceMetadataRetriever", splunkSourceMetadataRetriever);
		ReflectionTestUtils.setField(alertListDao, "numberOfDays", "10");
		Assert.assertTrue(alertListDao.getAlerts("test",1l,1,"test") != null);
	}

	@Test(expectedExceptions = AlertException.class)
	public void getAlertsAlertExceptionTest() throws AlertException {
		AlertListDao alertListDao = new AlertListDao();
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		AlertServiceRequest alertServiceRequest = Mockito
				.mock(AlertServiceRequest.class);
		AlertServiceResponse alertServiceResponse = Mockito
				.mock(AlertServiceResponse.class);
		Mockito.when(
				splunkSourceMetadataRetriever
						.getAlerts((AlertServiceRequest) Mockito.any()))
				.thenThrow(AlertException.class);
		Mockito.when(alertServiceResponse.getAlerts()).thenReturn(
				new ArrayList<ManagedAlert>());
		ReflectionTestUtils.setField(alertListDao,
				"splunkSourceMetadataRetriever", splunkSourceMetadataRetriever);
		ReflectionTestUtils.setField(alertListDao, "numberOfDays", "10");
		alertListDao.getAlerts("test",1l,1,"test");
	}

	@Test(expectedExceptions = AuthorizationException.class)
	public void getAlertAuthorizationExceptionTest() throws AlertException {
		AlertListDao alertListDao = new AlertListDao();
		alertListDao.getAlerts(null,0,0,null);
	}

	@Test
	public void getAlertsOverloadedMethodTest() throws AlertException {
		AlertListDao alertListDao = new AlertListDao();
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		AlertServiceRequest alertServiceRequest = Mockito
				.mock(AlertServiceRequest.class);
		AlertServiceResponse alertServiceResponse = Mockito
				.mock(AlertServiceResponse.class);
		Mockito.when(
				splunkSourceMetadataRetriever
						.getAlerts((AlertServiceRequest) Mockito.any()))
				.thenReturn(alertServiceResponse);
		Mockito.when(alertServiceResponse.getAlerts()).thenReturn(
				new ArrayList<ManagedAlert>());
		ReflectionTestUtils.setField(alertListDao,
				"splunkSourceMetadataRetriever", splunkSourceMetadataRetriever);
		Assert.assertTrue(alertListDao.getAlerts("test", 1l, 2l) != null);
	}

	@Test(expectedExceptions = AlertException.class)
	public void getAlertsOverloadedMethodAlertExceptionTest()
			throws AlertException {
		AlertListDao alertListDao = new AlertListDao();
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		AlertServiceRequest alertServiceRequest = Mockito
				.mock(AlertServiceRequest.class);
		AlertServiceResponse alertServiceResponse = Mockito
				.mock(AlertServiceResponse.class);
		Mockito.when(
				splunkSourceMetadataRetriever
						.getAlerts((AlertServiceRequest) Mockito.any()))
				.thenThrow(AlertException.class);
		Mockito.when(alertServiceResponse.getAlerts()).thenReturn(
				new ArrayList<ManagedAlert>());
		ReflectionTestUtils.setField(alertListDao,
				"splunkSourceMetadataRetriever", splunkSourceMetadataRetriever);
		alertListDao.getAlerts("test", 1l, 2l);

	}

	@Test(expectedExceptions = AuthorizationException.class)
	public void getAlertOverloadedMethodAuthorizationExceptionTest()
			throws AlertException {
		AlertListDao alertListDao = new AlertListDao();
		alertListDao.getAlerts(null, 0l, 0l);
	}

	@Test
	public void getSetOfAlertsTest() throws MetadataAccessException {
		AlertListDao alertListDao = new AlertListDao();
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		Set<String> set = Mockito.mock(Set.class);
		Mockito.when(metadataStore.getDataSources()).thenReturn(set);
		Mockito.when(set.isEmpty()).thenReturn(false);
		Iterator setIterator = Mockito.mock(Iterator.class);
		Mockito.when(set.iterator()).thenReturn(setIterator);
		Mockito.when(setIterator.hasNext()).thenReturn(true).thenReturn(false);
		Mockito.when(setIterator.next()).thenReturn("test");
		ReflectionTestUtils.setField(alertListDao, "metadataStore",
				metadataStore);
		ArrayNode arrayNode = alertListDao.getSetOfAlerts();
		Assert.assertEquals(arrayNode.get(0).get("label").getTextValue(),
				"test");
	}

	@Test(expectedExceptions = AuthorizationException.class)
	public void getSetOfAlertsAuthorizationExceptionTest()
			throws MetadataAccessException {
		AlertListDao alertListDao = new AlertListDao();
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		Set<String> set = Mockito.mock(Set.class);
		Mockito.when(metadataStore.getDataSources()).thenReturn(set);
		Mockito.when(set.isEmpty()).thenReturn(true);
		ReflectionTestUtils.setField(alertListDao, "metadataStore",
				metadataStore);
		alertListDao.getSetOfAlerts();
	}
	
	@Test
	public void getDatesTest() throws AlertException{
		AlertListDao alertListDao = new AlertListDao();
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever=Mockito.mock(SplunkSourceMetadataRetriever.class);
		Mockito.when(splunkSourceMetadataRetriever.getDates(Mockito.any(AlertServiceRequest.class))).thenReturn(Mockito.mock(List.class));
		ReflectionTestUtils.setField(alertListDao, "splunkSourceMetadataRetriever",splunkSourceMetadataRetriever);
		Assert.assertNotNull(alertListDao.getDates("test", 1l));
	}
	@Test(expectedExceptions = AlertException.class)
	public void getDatesAlertExceptionTest() throws AlertException{
		AlertListDao alertListDao = new AlertListDao();
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		Mockito.when(splunkSourceMetadataRetriever.getDates(Mockito.any(AlertServiceRequest.class))).thenThrow(AlertException.class);
		ReflectionTestUtils.setField(alertListDao,"splunkSourceMetadataRetriever", splunkSourceMetadataRetriever);
		alertListDao.getDates("test", 1l);
	}

	@Test
	public void getJSONTest(){
		AlertListDao alertListDao = new AlertListDao();
		HbaseJsonDataService hbaseJsonDataService=Mockito.mock(HbaseJsonDataService.class);
		Mockito.when(hbaseJsonDataService.getJSON(Mockito.any(String.class))).thenReturn(Mockito.mock(JsonData.class));
		ReflectionTestUtils.setField(alertListDao,"hbaseJsonDataService", hbaseJsonDataService);
		Assert.assertTrue(alertListDao.getJSON("test") instanceof JsonData);
	}
	
	@Test(expectedExceptions = AuthorizationException.class)
	public void getJSONAuthorizationExceptionTest(){
		AlertListDao alertListDao = new AlertListDao();
		HbaseJsonDataService hbaseJsonDataService=Mockito.mock(HbaseJsonDataService.class);
		ReflectionTestUtils.setField(alertListDao,"hbaseJsonDataService", hbaseJsonDataService);
	   alertListDao.getJSON(null) ;
	}
	
	@Test
	public void getHanndlerTest(){
		AlertListDao alertListDao = new AlertListDao();
		HbaseJsonDataService hbaseJsonDataService=Mockito.mock(HbaseJsonDataService.class);
		Mockito.when(hbaseJsonDataService.getHandler(Mockito.any(String.class))).thenReturn(Mockito.mock(Datahandler.class));
		ReflectionTestUtils.setField(alertListDao,"hbaseJsonDataService", hbaseJsonDataService);
		Assert.assertTrue(alertListDao.getHandler("test") instanceof Datahandler);
	}
	
	@Test(expectedExceptions = AuthorizationException.class)
	public void getHandlerAuthorizationExceptionTest(){
		AlertListDao alertListDao = new AlertListDao();
		HbaseJsonDataService hbaseJsonDataService=Mockito.mock(HbaseJsonDataService.class);
		ReflectionTestUtils.setField(alertListDao,"hbaseJsonDataService", hbaseJsonDataService);
	   alertListDao.getHandler(null) ;
	}
	
}
