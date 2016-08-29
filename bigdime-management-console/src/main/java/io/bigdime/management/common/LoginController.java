
/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
package io.bigdime.management.common;


import static io.bigdime.management.common.ApplicationConstants.TESTUSER;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/service/authenticate")
public class LoginController {
	
	@Value("${ldap.enabled}")
	private boolean ldapEnabled;
	@Autowired
	private AuthenticationService authenticationService;

	public AuthenticationService getAuthenticationService() {
		return authenticationService;
	}

	public void setAuthenticationService(
			AuthenticationService authenticationService) {
		this.authenticationService = authenticationService;
	}


	
	@RequestMapping(method = RequestMethod.POST, produces = "application/json")
	public @ResponseBody AuthenticateUser performUserValidation(@RequestBody final AuthenticateUser authenticateUser) {
		if (ldapEnabled) {
			authenticationService.authenticate(authenticateUser);
		} else {
			authenticateUser.setDisplayName(TESTUSER);
			authenticateUser.setLoginStatus(true);
		}			
		return authenticateUser;
		
	}
}
