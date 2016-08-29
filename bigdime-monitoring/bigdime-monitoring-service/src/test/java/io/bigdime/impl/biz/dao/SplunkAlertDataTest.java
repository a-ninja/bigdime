/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.SOURCE_TYPE;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.AlertData;
import io.bigdime.impl.biz.dao.SplunkAlertData;
import io.bigdime.splunkalert.SplunkAlert;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class SplunkAlertDataTest {
	private static final Logger logger = LoggerFactory.getLogger(SplunkAlertDataTest.class);

	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE,"Test Phase","Setting the environment");
		System.setProperty(ENVIORNMENT, "test");
	}
	@Test
	public void raisedAlertsGettersandSettersTest() {
		SplunkAlertData splunkAlertData = new SplunkAlertData();
		SplunkAlert splunkAlert=new SplunkAlert();
		splunkAlert.setApplicationName("test");
		List<SplunkAlert> list = new ArrayList<SplunkAlert>();
		list.add(splunkAlert);
		splunkAlertData.setRaisedAlerts(list);
		Assert.assertTrue(splunkAlertData.getRaisedAlerts().get(0).getApplicationName().equals("test"));

	}
	
}
