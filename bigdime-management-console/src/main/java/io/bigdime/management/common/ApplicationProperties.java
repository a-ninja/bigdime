/**
 * Copyright (C) 2015 Stubhub.
 */

/**
 * 
 * @author  Sandeep Reddy,Murthy
 *
 */

package io.bigdime.management.common;

public class ApplicationProperties {
	
	private String devHost;
	private String qaHost;
	private String prodHost;
	private String devPort;
	private String qaPort;
	private String prodPort;
	private String application;
	private String rowsPerPage;
	
	public String getDevHost() {
		return devHost;
	}
	public void setDevHost(String devHost) {
		this.devHost = devHost;
	}
	public String getQaHost() {
		return qaHost;
	}
	public void setQaHost(String qaHost) {
		this.qaHost = qaHost;
	}
	public String getProdHost() {
		return prodHost;
	}
	public void setProdHost(String prodHost) {
		this.prodHost = prodHost;
	}
	public String getDevPort() {
		return devPort;
	}
	public void setDevPort(String devPort) {
		this.devPort = devPort;
	}
	public String getQaPort() {
		return qaPort;
	}
	public void setQaPort(String qaPort) {
		this.qaPort = qaPort;
	}
	public String getProdPort() {
		return prodPort;
	}
	public void setProdPort(String prodPort) {
		this.prodPort = prodPort;
	}
	
	public String getApplication() {
		return application;
	}
	public void setApplication(String application) {
		this.application = application;
	}
	public String getRowsPerPage() {
		return rowsPerPage;
	}
	public void setRowsPerPage(String rowsPerPage) {
		this.rowsPerPage = rowsPerPage;
	}
	
	

}
