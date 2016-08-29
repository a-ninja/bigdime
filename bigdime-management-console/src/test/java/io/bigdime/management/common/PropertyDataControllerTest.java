/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.management.common;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PropertyDataControllerTest {
	private static final Logger logger = LoggerFactory
			.getLogger(PropertyDataControllerTest.class);
	
	PropertyDataController propertyDataController;
	
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
	
	@Test
	public void getApplicationPropertiesTest(){
		propertyDataController=new PropertyDataController();
		ReflectionTestUtils.setField(propertyDataController, "devHost", "devHost");
		ReflectionTestUtils.setField(propertyDataController, "qaHost", "qaHost");
		ReflectionTestUtils.setField(propertyDataController, "prodHost", "prodHost");
		ReflectionTestUtils.setField(propertyDataController, "devPort", "devPort");
		ReflectionTestUtils.setField(propertyDataController, "qaPort", "qaPort");
		ReflectionTestUtils.setField(propertyDataController, "prodPort", "prodPort");	
		ReflectionTestUtils.setField(propertyDataController, "rowsPerPage", "rowsPerPage");
		ApplicationProperties applicationProperties =propertyDataController.getApplicationProperties();
		Assert.assertTrue(applicationProperties.getDevHost().equals("devHost"));
		Assert.assertTrue(applicationProperties.getQaHost().equals("qaHost"));
		Assert.assertTrue(applicationProperties.getProdHost().equals("prodHost"));
		Assert.assertTrue(applicationProperties.getDevPort().equals("devPort"));
		Assert.assertTrue(applicationProperties.getQaPort().equals("qaPort"));
		Assert.assertTrue(applicationProperties.getProdPort().equals("prodPort"));
		Assert.assertTrue(applicationProperties.getRowsPerPage().equals("rowsPerPage"));
	}
}
