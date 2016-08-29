/**
 * Copyright (C) 2015 Stubhub.
 * @author Sandeep Reddy,Murthy
 */
package io.bigdime.management.common;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.mockito.Mockito;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.management.common.ApplicationConstants.TESTUSER;

public class LoginControllerTest extends PowerMockTestCase {
	private static final Logger logger = LoggerFactory
			.getLogger(LoginControllerTest.class);
	
	LoginController loginController;
	
//	@BeforeClass
//	public void init() {
//		loginController = new LoginController();
//	}

	@BeforeTest
	public void setup() {
		logger.info("source type", "Test Phase", "Setting the environment");
		System.setProperty("env", "test");
	}
	
	@Test
	public void getAuthenticationServiceTest(){
		loginController = new LoginController();
		AuthenticationService authenticationService=new AuthenticationService();
		loginController.setAuthenticationService(authenticationService);
		Assert.assertSame(authenticationService, loginController.getAuthenticationService());		
	}
	
	@Test
	public void performUserValidationLdapEnabledTest(){
		loginController = new LoginController();
		AuthenticationService authenticationService=Mockito.mock(AuthenticationService.class);
		AuthenticateUser authenticateUser=new AuthenticateUser();
		authenticateUser.setUserId("testuser");
		authenticateUser.setPassword("testpwd");
		Mockito.when(authenticationService.authenticate(Mockito.any(AuthenticateUser.class))).thenReturn(authenticateUser);
		ReflectionTestUtils.setField(loginController, "ldapEnabled", true);
		ReflectionTestUtils.setField(loginController,"authenticationService", authenticationService);
		AuthenticateUser authenticatedUser=loginController.performUserValidation(authenticateUser);
		Assert.assertEquals(authenticatedUser.getUserId(), "testuser");
		Assert.assertEquals(authenticatedUser.getPassword(), "testpwd");		
	}
	
	@Test
	public void performUserValidationLdapDisabledTest(){
		loginController = new LoginController();
		AuthenticationService authenticationService=Mockito.mock(AuthenticationService.class);
		AuthenticateUser authenticateUser=new AuthenticateUser();
		Mockito.when(authenticationService.authenticate(Mockito.any(AuthenticateUser.class))).thenReturn(authenticateUser);
		ReflectionTestUtils.setField(loginController, "ldapEnabled", false);
		ReflectionTestUtils.setField(loginController,"authenticationService", authenticationService);
		AuthenticateUser authenticatedUser=loginController.performUserValidation(authenticateUser);
		Assert.assertEquals(authenticatedUser.getDisplayName(),TESTUSER );
		Assert.assertEquals(authenticatedUser.getLoginStatus(), true);	
	}
}
