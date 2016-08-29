package io.bigdime.impl.biz.dao;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.Channel;
import io.bigdime.impl.biz.dao.ChannelHandler;
import io.bigdime.impl.biz.dao.Properties;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;

public class ChannelTest {
	private static final Logger logger = LoggerFactory.getLogger(ChannelTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void channelGettersAndSettersTest(){
		Channel channel=new Channel();
		List<ChannelHandler> channelHandlers=new ArrayList<ChannelHandler>();
		ChannelHandler channelHandler=new ChannelHandler();
		channelHandler.setChannelclass(TEST_STRING);
		channelHandler.setDescription(TEST_STRING);
		channelHandler.setName(TEST_STRING);
		channelHandler.setProperties(new Properties());
		channelHandlers.add(channelHandler);
		channel.setChannelHandlers(channelHandlers);
		
		for(ChannelHandler channel_Handler : channel.getChannelHandlers()){
		Assert.assertEquals(TEST_STRING, channel_Handler.getChannelclass());
		Assert.assertEquals(TEST_STRING, channel_Handler.getDescription());
		Assert.assertEquals(TEST_STRING, channel_Handler.getName());
		Assert.assertTrue(channel_Handler.getProperties() instanceof Properties);
		}
		
	}
	
}
