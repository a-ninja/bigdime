/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.service;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.ENVIRONMENT_VALUE;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.SOURCE_TYPE;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.alert.AlertException;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.common.ConnectionFactory;
import io.bigdime.impl.biz.dao.AlertData;
import io.bigdime.impl.biz.dao.AlertListDao;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.JsonData;
import io.bigdime.impl.biz.dao.SplunkAlertData;
import io.bigdime.impl.biz.exception.AuthorizationException;
import io.bigdime.impl.biz.serviceImpl.AlertDataResponderServiceImpl;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jackson.node.ArrayNode;
import org.eclipse.jetty.http.HttpStatus;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;

/**
 * 
 * @author samurthy
 * 
 */
@PrepareForTest({ArrayNode.class})
public class AlertDataResponderServiceImplTest extends PowerMockTestCase{
	private static final Logger logger = LoggerFactory
			.getLogger(AlertDataResponderServiceImplTest.class);

	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE, "Test Phase", "Setting the environment");
		System.setProperty(ENVIORNMENT, ENVIRONMENT_VALUE);
	}

	@Test
	public void getSplunkAlertsTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		SplunkAlertData splunkAlertData = Mockito.mock(SplunkAlertData.class);
		Mockito.when(alertListDao.getSplunkAlerts())
				.thenReturn(splunkAlertData);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertNotNull(alertDataResponderServiceImpl.getSplunkAlerts());
	}

	@Test
	public void getSplunkAlertsExceptionTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getSplunkAlerts()).thenThrow(Exception.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertNotNull(alertDataResponderServiceImpl.getSplunkAlerts());
	}

	@Test
	public void getAlertsTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		AlertData alertData = Mockito.mock(AlertData.class);
		Mockito.when(alertListDao.getAlerts(Mockito.anyString(),Mockito.anyLong(),Mockito.anyInt(),Mockito.any(String.class))).thenReturn(
				alertData);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertNotNull(alertDataResponderServiceImpl.getAlerts("test",1l,1,"test"));
	}
	
	@Test
	public void getAlertsAuthorizationExceptionTest() throws AlertException  {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getAlerts(Mockito.anyString(),Mockito.anyLong(),Mockito.anyInt(),Mockito.anyString())).thenThrow(
				AuthorizationException.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getAlerts("test",1l,1,"test").getStatus(),HttpStatus.NOT_ACCEPTABLE_406);
	}

	@Test
	public void getAlertsExceptionTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getAlerts(Mockito.anyString(),Mockito.anyLong(),Mockito.anyInt(),Mockito.anyString())).thenThrow(
				Exception.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getAlerts("test",1l,1,"test").getStatus(),HttpStatus.SERVICE_UNAVAILABLE_503);
	}

	@Test
	public void getAlertsOverloadedTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		AlertData alertData = Mockito.mock(AlertData.class);
		Mockito.when(
				alertListDao.getAlerts(Mockito.anyString(), Mockito.anyLong(),
						Mockito.anyLong())).thenReturn(alertData);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertNotNull(alertDataResponderServiceImpl.getAlerts("test",
				1l, 2l));
	}

	@Test
	public void getAlertsOverloadedExceptionTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(
				alertListDao.getAlerts(Mockito.anyString(), Mockito.anyLong(),
						Mockito.anyLong())).thenThrow(Exception.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getAlerts("test",1l, 2l).getStatus(),HttpStatus.SERVICE_UNAVAILABLE_503);
	}
	

	@Test
	public void getAlertsOverloadedMethodAuthorizationExceptionTest() throws Exception {
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(
				alertListDao.getAlerts(Mockito.anyString(), Mockito.anyLong(),
						Mockito.anyLong())).thenThrow(AuthorizationException.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getAlerts("test",1l, 2l).getStatus(),HttpStatus.NOT_ACCEPTABLE_406);
	}
	
	@Test
    public void getSetOfAlertsTest() throws MetadataAccessException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		ArrayNode arrayNode=Mockito.mock(ArrayNode.class);
		Mockito.when(alertListDao.getSetOfAlerts()).thenReturn(arrayNode);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertNotNull(alertDataResponderServiceImpl.getSetOfAlerts());
	}
	
	@Test
	public void getSetOfAlertsAuthorizationExceptionTest() throws MetadataAccessException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		ArrayNode arrayNode=Mockito.mock(ArrayNode.class);
		Mockito.when(alertListDao.getSetOfAlerts()).thenReturn(arrayNode);
		Mockito.when(alertListDao.getSetOfAlerts()).thenThrow(AuthorizationException.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getSetOfAlerts().getStatus(),HttpStatus.NOT_ACCEPTABLE_406);
	}
	
	@Test
	public void getSetOfAlertsExceptionTest() throws MetadataAccessException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		ArrayNode arrayNode=Mockito.mock(ArrayNode.class);
		Mockito.when(alertListDao.getSetOfAlerts()).thenReturn(arrayNode);
		Mockito.when(alertListDao.getSetOfAlerts()).thenThrow(Exception.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,
				"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getSetOfAlerts().getStatus(),HttpStatus.SERVICE_UNAVAILABLE_503);
	}
	
	@Test
	public void getDatesTest() throws AlertException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		ArrayNode arrayNode=Mockito.mock(ArrayNode.class);
		Mockito.when(alertListDao.getDates(Mockito.anyString(), Mockito.anyLong())).thenReturn(Mockito.mock(List.class));;
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getDates("test",1l).getStatus(),HttpStatus.OK_200);
	} 
	@Test
	public void getDatesAlertExceptionTest() throws AlertException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getDates(Mockito.anyString(), Mockito.anyLong())).thenThrow(AlertException.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getDates("test",1l).getStatus(),HttpStatus.NOT_ACCEPTABLE_406);
	}
		
	@Test
	public void getJSONTest() throws AlertException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getJSON(Mockito.anyString())).thenReturn(Mockito.mock(JsonData.class));;
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getJSON("test").getStatus(),HttpStatus.OK_200);
	} 
	@Test
	public void getJSONAuthorizationExceptionTest(){
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getJSON(Mockito.anyString())).thenThrow(AuthorizationException.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getJSON("test").getStatus(),HttpStatus.NOT_ACCEPTABLE_406);
	}
	
	@Test
	public void getJSONExceptionTest(){
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getJSON(Mockito.anyString())).thenThrow(Exception.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getJSON("test").getStatus(),HttpStatus.SERVICE_UNAVAILABLE_503);
	}
	
	
	@Test
	public void getHandlerTest() throws AlertException{
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getHandler(Mockito.anyString())).thenReturn(Mockito.mock(Datahandler.class));;
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getHandler("test").getStatus(),HttpStatus.OK_200);
	} 
	@Test
	public void getHandlerAuthorizationExceptionTest(){
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getHandler(Mockito.anyString())).thenThrow(AuthorizationException.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getHandler("test").getStatus(),HttpStatus.NOT_ACCEPTABLE_406);
	}
	
	@Test
	public void getHandlerExceptionTest(){
		AlertDataResponderServiceImpl alertDataResponderServiceImpl = new AlertDataResponderServiceImpl();
		AlertListDao alertListDao = Mockito.mock(AlertListDao.class);
		Mockito.when(alertListDao.getHandler(Mockito.anyString())).thenThrow(Exception.class);
		ReflectionTestUtils.setField(alertDataResponderServiceImpl,"alertListDao", alertListDao);
		Assert.assertEquals(alertDataResponderServiceImpl.getHandler("test").getStatus(),HttpStatus.SERVICE_UNAVAILABLE_503);
	}
	
}
