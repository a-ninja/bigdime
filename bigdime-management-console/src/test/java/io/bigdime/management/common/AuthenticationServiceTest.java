/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.management.common;

import java.util.Hashtable;
import java.util.Iterator;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@PrepareForTest({ AuthenticationService.class })
public class AuthenticationServiceTest extends PowerMockTestCase {

	AuthenticationService authenticationService;

	@BeforeClass
	public void init() {
		authenticationService = new AuthenticationService();
	}

	@BeforeTest
	public void setup() {
		System.setProperty("env", "test");
	}

	@Test
	public void authenticateTest() throws Exception {
		InitialDirContext initialDirContext = Mockito
				.mock(InitialDirContext.class);
		NamingEnumeration results = Mockito.mock(NamingEnumeration.class);
		Mockito.when(
				initialDirContext.search(Mockito.any(String.class),
						Mockito.any(String.class),
						Mockito.any(SearchControls.class))).thenReturn(results);
		Mockito.when(results.hasMore()).thenReturn(true).thenReturn(true)
				.thenReturn(false);
		SearchResult result = Mockito.mock(SearchResult.class);
		Mockito.when(results.next()).thenReturn(result);
		Mockito.when(result.getName()).thenReturn("test");
		Attributes attributes = Mockito.mock(Attributes.class);
		Mockito.when(result.getAttributes()).thenReturn(attributes);
		Attribute attribute = Mockito.mock(Attribute.class);
		Mockito.when(attributes.get(Mockito.anyString())).thenReturn(attribute);
		Mockito.when(attribute.get()).thenReturn("test");
		AuthenticateUser authenticateUser = Mockito
				.mock(AuthenticateUser.class);
		Mockito.when(authenticateUser.getPassword()).thenReturn("test");
		ReflectionTestUtils.setField(authenticationService, "serviceAccount",
				"test");
		ReflectionTestUtils.setField(authenticationService, "serviceAccountPW",
				"test");
		ReflectionTestUtils.setField(authenticationService, "ldapURL",
				"ldap://test.test.com:123");
		PowerMockito.whenNew(InitialDirContext.class)
				.withArguments((Hashtable) Mockito.any())
				.thenReturn(initialDirContext);
		Assert.assertEquals("test",
				authenticationService.authenticate(authenticateUser)
						.getPassword());
	}

	@Test
	public void getContextTest() throws Exception {
		ReflectionTestUtils.setField(authenticationService, "serviceAccount",
				"test");
		ReflectionTestUtils.setField(authenticationService, "serviceAccountPW",
				"test");
		ReflectionTestUtils.setField(authenticationService, "ldapURL",
				"ldap://test.test.com:123");
		InitialDirContext initialDirContext = Mockito
				.mock(InitialDirContext.class);
		PowerMockito.whenNew(InitialDirContext.class)
				.withArguments((Hashtable) Mockito.any())
				.thenReturn(initialDirContext);
		Assert.assertNotNull(authenticationService.getContext("test", "test",
				true));
	}

}
