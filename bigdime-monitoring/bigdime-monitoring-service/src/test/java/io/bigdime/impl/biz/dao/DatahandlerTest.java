package io.bigdime.impl.biz.dao;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.Properties;
import org.testng.Assert;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;

public class DatahandlerTest {
	
	private static final Logger logger = LoggerFactory.getLogger(DatahandlerTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void dataHandlerGettersAndSettersTest(){
		
		Datahandler dataHandler=new Datahandler();
		dataHandler.setName(TEST_STRING);
		dataHandler.setDescription(TEST_STRING);
		dataHandler.setChannelclass(TEST_STRING);
		dataHandler.setHandlerclass(TEST_STRING);
		dataHandler.setProperties(new Properties());
		
		Assert.assertEquals(dataHandler.getName(), TEST_STRING);
		Assert.assertEquals(dataHandler.getDescription(), TEST_STRING);
		Assert.assertEquals(dataHandler.getHandlerclass(), TEST_STRING);
		Assert.assertEquals(dataHandler.getChannelclass(), TEST_STRING);
		Assert.assertTrue(dataHandler.getProperties() instanceof Properties);
	}

}
