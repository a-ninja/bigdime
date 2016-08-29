/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * 
 * @author  Sandeep Reddy,Murthy,vchevendra
 *
 */
package io.bigdime.management.common;

import org.springframework.stereotype.Component;

@Component
public class AuthenticateUser {
	private boolean loginStatus = false;
	private String displayName;
	private String userId;
	private String password;

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	private String errorCode = null;

	public AuthenticateUser() {
	}

	public AuthenticateUser(String userId) {
		this.userId = userId;

	}

	public boolean getLoginStatus() {
		return loginStatus;
	}

	public void setLoginStatus(boolean loginStatus) {
		this.loginStatus = loginStatus;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

}
