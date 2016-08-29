package io.bigdime.impl.biz.dao;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.SourceDescription;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;
public class SourceDescriptionTest {
	private static final Logger logger = LoggerFactory.getLogger(SourceDescriptionTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void sourceDescriptionGettersAndSettersTest(){
		SourceDescription sourceDescription=new SourceDescription();
		Map<String,String> keyvalues=new HashMap<String,String>();
		keyvalues.put(TEST_STRING, TEST_STRING);
		sourceDescription.setKeyvalues(keyvalues);
		Assert.assertEquals(TEST_STRING,sourceDescription.getKeyvalues().get(TEST_STRING));
	}
}
