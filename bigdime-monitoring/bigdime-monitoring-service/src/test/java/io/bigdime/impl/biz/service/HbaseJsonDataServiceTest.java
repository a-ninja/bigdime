package io.bigdime.impl.biz.service;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.mockito.Mockito;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.impl.biz.dao.Adaptor;
import io.bigdime.impl.biz.dao.AdaptorConstants;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.JsonData;
import io.bigdime.impl.biz.service.HbaseJsonDataService;

public class HbaseJsonDataServiceTest extends PowerMockTestCase{
	
	private static final Logger logger = LoggerFactory.getLogger(HbaseJsonDataServiceTest.class);

	@BeforeTest
	public void setup() {
		System.setProperty("env", "test");
	}	
	@Test
	public void getJSONTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result=Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenReturn(result);
		ObjectNode objectNode=Mockito.mock(ObjectNode.class);
		ObjectMapper objectMapper=Mockito.mock(ObjectMapper.class);
		Mockito.when(result.getValue(Mockito.any(byte[].class), Mockito.any(byte[].class))).thenReturn("{\"name\":\"value\"}".getBytes());
		Mockito.when(objectMapper.readTree(Mockito.any(String.class))).thenReturn(objectNode);
		Mockito.when(objectMapper.readValue(Mockito.any(String.class),Mockito.eq(JsonData.class))).thenReturn(Mockito.mock(JsonData.class));
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		Assert.assertTrue(hbaseJsonDataService.getJSON("test") instanceof JsonData);
	}
	
	@Test
	public void getJSONHBaseClientExceptionTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result=Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenThrow(HBaseClientException.class);
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		Assert.assertNull(hbaseJsonDataService.getJSON("test"));
	}
	@Test
	public void getJSONExceptionTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result=Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenThrow(Exception.class);
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		Assert.assertNull(hbaseJsonDataService.getJSON("test"));
	}
	
	@Test
	public void getHandlerTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result=Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenReturn(result);
		ObjectNode objectNode=Mockito.mock(ObjectNode.class);
		ObjectMapper objectMapper=Mockito.mock(ObjectMapper.class);
		Mockito.when(result.getValue(Mockito.any(byte[].class), Mockito.any(byte[].class))).thenReturn("{\"name\":\"value\"}".getBytes());
		Mockito.when(objectMapper.readTree(Mockito.any(String.class))).thenReturn(objectNode);
		Mockito.when(objectMapper.readValue(Mockito.any(String.class),Mockito.eq(Datahandler.class))).thenReturn(Mockito.mock(Datahandler.class));
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		Assert.assertTrue(hbaseJsonDataService.getHandler("test") instanceof Datahandler);
	}
	
	@Test
	public void getHandlerHBaseClientExceptionTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result=Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenThrow(HBaseClientException.class);
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		Assert.assertNull(hbaseJsonDataService.getHandler("test"));
	}
	@Test
	public void getHandlerExceptionTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result=Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenThrow(Exception.class);
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		Assert.assertNull(hbaseJsonDataService.getHandler("test"));
	}
	
	@Test
	public void getAdaptorConstantsTest() throws HBaseClientException, IOException{
		HbaseJsonDataService hbaseJsonDataService=new HbaseJsonDataService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager).retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner=Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);	
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(result.containsColumn(Mockito.any(byte[].class),Mockito.any(byte[].class))).thenReturn(true);
		Mockito.when(result.getValue(Mockito.any(byte[].class),Mockito.any(byte[].class))).thenReturn(Bytes.toBytes("test"));
		ReflectionTestUtils.setField(hbaseJsonDataService, "hbaseManager",hbaseManager);
		for( AdaptorConstants adaptorConstants:hbaseJsonDataService.getAdaptorConstants()){
			Assert.assertEquals(adaptorConstants.getEnvironment(), "test");
			for(Adaptor adaptor:adaptorConstants.getAdaptorList()){
				Assert.assertEquals(adaptor.getName(), "test");
			}
			break;
		}
	}

}
