package io.bigdime.impl.biz.dao;

import org.testng.Assert;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.ChannelHandler;
import io.bigdime.impl.biz.dao.Properties;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;
public class ChannelHandlerTest {
	
	private static final Logger logger = LoggerFactory.getLogger(ChannelHandlerTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void channelHandlerGettersAndSettersTest(){
		ChannelHandler channelHandler=new ChannelHandler();
		channelHandler.setName(TEST_STRING);
		channelHandler.setDescription(TEST_STRING);
		channelHandler.setChannelclass(TEST_STRING);
		channelHandler.setProperties(new Properties());
	
		Assert.assertEquals(TEST_STRING,channelHandler.getName());
		Assert.assertEquals(TEST_STRING,channelHandler.getDescription());
		Assert.assertEquals(TEST_STRING,channelHandler.getChannelclass());
		Assert.assertTrue(channelHandler.getProperties() instanceof Properties);
	}

}
