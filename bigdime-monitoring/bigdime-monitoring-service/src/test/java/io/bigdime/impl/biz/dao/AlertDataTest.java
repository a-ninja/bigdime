/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.dao;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.SOURCE_TYPE;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.impl.biz.dao.AlertData;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.ManagedAlert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class AlertDataTest{

	private static final Logger logger = LoggerFactory
			.getLogger(AlertDataTest.class);

	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE,"Test Phase","Setting the environment");
		System.setProperty(ENVIORNMENT, "test");

	}

	@Test
	public void raisedAlertsGettersandSettersTest() {
		AlertData alertData = new AlertData();
		List<ManagedAlert> list = new ArrayList<ManagedAlert>();
		alertData.setRaisedAlerts(list);
		alertData.getRaisedAlerts();

	}

}
