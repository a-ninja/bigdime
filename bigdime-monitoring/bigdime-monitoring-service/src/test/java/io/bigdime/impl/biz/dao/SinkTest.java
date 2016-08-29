package io.bigdime.impl.biz.dao;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.Sink;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;

public class SinkTest {
	
	private static final Logger logger = LoggerFactory.getLogger(SinkTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void sinkGetterAndSettersTest(){
		Sink sink=new Sink();
		sink.setName(TEST_STRING);
		sink.setDescription(TEST_STRING);
		List<String> channelDescList=new ArrayList<String>();
		channelDescList.add(TEST_STRING);
		List<Datahandler> dataHandlerList=new ArrayList<Datahandler>();
		Datahandler dataHandler=new Datahandler();
		dataHandler.setDescription(TEST_STRING);
		dataHandler.setName(TEST_STRING);
		dataHandlerList.add(dataHandler);
		sink.setChanneldesc(channelDescList);
		sink.setDatahandlers(dataHandlerList);
		Assert.assertEquals(TEST_STRING, sink.getDescription());
		Assert.assertEquals(TEST_STRING, sink.getName());
		for(String channelDescription:sink.getChanneldesc()){
			Assert.assertEquals(TEST_STRING,channelDescription );
		}
		
		for(Datahandler datahandler:sink.getDatahandlers()){
			Assert.assertEquals(TEST_STRING,datahandler.getDescription() );
			Assert.assertEquals(TEST_STRING,datahandler.getName());
			
		}
		
	}

}
