/**
 * Copyright (C) 2015 Stubhub.
 */

/**
 * 
 * @author  Sandeep Reddy,Murthy
 *
 */
package io.bigdime.management.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/propertyService/applicationproperties")
public class PropertyDataController {

	@Value("${monitoring.devhost}")
	private String devHost;
	@Value("${monitoring.qahost}")
	private String qaHost;
	@Value("${monitoring.prodhost}")
	private String prodHost;
	@Value("${monitoring.devport}")
	private String devPort;
	@Value("${monitoring.qaport}")
	private String qaPort;
	@Value("${monitoring.prodport}")
	private String prodPort;
	@Value("${monitoring.application}")
	private String application;
	@Value("${management.numberofrowsperpage}")
	private String rowsPerPage;
	@RequestMapping(method = RequestMethod.GET, produces = "application/json")
	public @ResponseBody ApplicationProperties getApplicationProperties() {
		ApplicationProperties applicationProperties = new ApplicationProperties();
		applicationProperties.setDevHost(devHost);
		applicationProperties.setQaHost(qaHost);
		applicationProperties.setProdHost(prodHost);
		applicationProperties.setDevPort(devPort);
		applicationProperties.setQaPort(qaPort);
		applicationProperties.setProdPort(prodPort);
		applicationProperties.setApplication(application);
		applicationProperties.setRowsPerPage(rowsPerPage);
		return applicationProperties;

	}

}
