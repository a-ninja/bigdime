package io.bigdime.ext.swift;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SwiftConnectionFactory {

	@Value("${swift.user.name}")
	private String userName;
	@Value("${swift.password}")
	private String password;
	@Value("${swift.auth.url}")
	private String authUrl;
	@Value("${swift.tenant.id}")
	private String tenantId;
	@Value("${swift.tenant.name}")
	private String tenantName;
	@Value("${swift.container.name}")
	private String containerName;
	@Value("${swift.alert.container.name}")
	private String alertContainerName;
	@Value("${swift.alert.level}")
	private String alertLevel;

	public Account getSwiftAccount() {
		AccountConfig config = new AccountConfig();
		config.setUsername(userName);
		config.setPassword(password);
		config.setAuthUrl(authUrl);
		config.setTenantId(tenantId);
		config.setTenantName(tenantName);
		Account account = new AccountFactory(config).createAccount();
		return account;
	}
}
