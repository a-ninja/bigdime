package io.bigdime.impl.biz.dao;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.ChannelHandler;
import io.bigdime.impl.biz.dao.JsonData;
import io.bigdime.impl.biz.dao.Sink;
import io.bigdime.impl.biz.dao.Source;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;

public class JsonDataTest {
	
	private static final Logger logger = LoggerFactory.getLogger(JsonData.class);
	
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void jsonDataGettersAndSettersTest(){
		JsonData jsonData =new JsonData();
		List<Sink> sinkList=new ArrayList<Sink>();
		List<ChannelHandler> channelHandler=new ArrayList<ChannelHandler>();
		jsonData.setName(TEST_STRING);
		jsonData.setType(TEST_STRING);
		jsonData.setCronexpression(TEST_STRING);
		jsonData.setAutostart(TEST_STRING);
		jsonData.setNamespace(TEST_STRING);
		jsonData.setDescription(TEST_STRING);
		jsonData.setSource(new Source());
		jsonData.setSink(sinkList);
		Sink sink=new Sink();
		sink.setDescription(TEST_STRING);
		sink.setName(TEST_STRING);
		sinkList.add(sink);
		jsonData.setChannel(channelHandler);
		Assert.assertEquals(TEST_STRING, jsonData.getName());
		Assert.assertEquals(TEST_STRING, jsonData.getType());
		Assert.assertEquals(TEST_STRING, jsonData.getCronexpression());
		Assert.assertEquals(TEST_STRING, jsonData.getAutostart());
		Assert.assertEquals(TEST_STRING, jsonData.getNamespace());
		Assert.assertEquals(TEST_STRING, jsonData.getDescription());
		Assert.assertTrue(jsonData.getSource() instanceof Source);
		
		for(Sink sinkObj:jsonData.getSink()){
		Assert.assertEquals(TEST_STRING,sinkObj.getDescription());
		Assert.assertEquals(TEST_STRING, sinkObj.getName());
		}
	}

}
