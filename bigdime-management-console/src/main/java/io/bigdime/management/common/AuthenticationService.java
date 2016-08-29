/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * Provides user authentication services
 * @author Sandeep Reddy,Murthy
 *
 */
package io.bigdime.management.common;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.ModelAndView;

@Component
public class AuthenticationService {
	static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(AuthenticationService.class);

	@Value("${ldap.platform.accountName}")
	private String serviceAccount;

	@Value("${ldap.platform.account.pw}")
	private String serviceAccountPW;

	@Value("${ldap.url}")
	private String ldapURL;
	
	@Value("${ldap.BASE}")
	private String base;

	/**
	 * 
	 * @param authenticateUser
	 * @param password
	 * @return
	 */

	public AuthenticateUser authenticate(AuthenticateUser authenticateUser) {
		DirContext ctx;
		try {
			ctx = getContext(serviceAccount, serviceAccountPW, true);
			SearchControls sc = new SearchControls();
			sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
			NamingEnumeration<SearchResult> results = ctx.search(
					base,
					"(&(objectClass=*)(sAMAccountName="
							+ authenticateUser.getUserId() + "))", sc);
			while (results.hasMore()) {
				SearchResult result = results.next();
				String dn = result.getName() + ",";
				try {
					DirContext context = getContext(dn,
							authenticateUser.getPassword(), false);
					NamingEnumeration<SearchResult> results1 = context.search(
							base,
							"(&(objectClass=*)(sAMAccountName="
									+ authenticateUser.getUserId() + "))", sc);
					if (results1.hasMore()) {
						SearchResult searchResults = results1.next();

						Attributes attributes = searchResults.getAttributes();
						Attribute attr = attributes.get("cn");
						String dispName = (String) attr.get();
						authenticateUser.setDisplayName(dispName);
					}

					context.close();
					authenticateUser.setLoginStatus(true);

				} catch (javax.naming.NamingException e) {
					logger.warn("BIGDIME-MANAGEMENT-CONSOLE",
							"Error occured calling LDAP", e.getMessage());
					ctx.close();
				}
			}
			ctx.close();
		} catch (NamingException e) {
			logger.warn("BIGDIME-MANAGEMENT-CONSOLE",
					"Error occured calling LDAP", e.getMessage());
		}
		return authenticateUser;
	}

	/**
	 * 
	 * @param dn
	 * @param password
	 * @param serviceAcc
	 * @return
	 * @throws NamingException
	 */
	public DirContext getContext(String dn, String password, boolean serviceAcc)
			throws NamingException {
		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,
				"com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, ldapURL);
		if (serviceAcc) {
			env.put(Context.SECURITY_PRINCIPAL, "CN=" + serviceAccount
					+ ",OU=ServiceAccts,"+base);
		} else {
			env.put(Context.SECURITY_PRINCIPAL, dn + base);
		}
		env.put(Context.REFERRAL, "follow");
		env.put(Context.SECURITY_CREDENTIALS, password);
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		DirContext initial = new InitialDirContext(env);
		return initial;
	}
}
