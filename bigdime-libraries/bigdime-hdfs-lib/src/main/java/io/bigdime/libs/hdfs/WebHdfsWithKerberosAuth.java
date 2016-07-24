/**
 * Copyright (C) 2015 Stubhub.
 */

package io.bigdime.libs.hdfs;

import java.io.IOException;
import java.security.Principal;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author adwang, Neeraj Jain, mnamburi
 *
 */
public class WebHdfsWithKerberosAuth extends WebHdfs {
	private static Logger logger = LoggerFactory.getLogger(WebHdfsWithKerberosAuth.class);

	private static String DEFAULT_KRB5_CONFIG_LOCATION = "/etc/krb5.conf";

	protected WebHdfsWithKerberosAuth(String host, int port) {
		super(host, port);
		String krb5ConfigPath = System.getProperty("java.security.krb5.conf");
		if (krb5ConfigPath == null) {
			krb5ConfigPath = DEFAULT_KRB5_CONFIG_LOCATION;
		}
		boolean skipPortAtKerberosDatabaseLookup = true;
		System.setProperty("java.security.krb5.conf", krb5ConfigPath);
		System.setProperty("sun.security.krb5.debug", "true");
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider> create()
				.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(skipPortAtKerberosDatabaseLookup)).build();

		setHttpClient(HttpClientBuilder.create().setDefaultAuthSchemeRegistry(authSchemeRegistry).build());// new

//		this.addParameter("anonymous", "true");
	}

	public static WebHdfsWithKerberosAuth getInstance(String host, int port) {
		return new WebHdfsWithKerberosAuth(host, port);
	}

	// LISTSTATUS, OPEN, GETFILESTATUS, GETCHECKSUM,
	protected HttpResponse get() throws ClientProtocolException, IOException {
		logger.debug("WebHdfsWithKerberosAuth getting");
		HttpClientContext context = HttpClientContext.create();
		BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		Credentials useJaasCreds = new Credentials() {
			public String getPassword() {
				return null;
			}

			public Principal getUserPrincipal() {
				return null;
			}
		};

		credentialsProvider.setCredentials(new AuthScope(null, -1, null), useJaasCreds);
		context.setCredentialsProvider(credentialsProvider);
		// this.addParameter("anonymous=true", "true");
		logger.debug("WebHdfsWithKerberosAuth getting from:{}", uri);
		httpRequest = new HttpGet(uri);
		logger.debug("File status request: {}", httpRequest.getURI());
		uri = null;

		return httpClient.execute(httpRequest, context);
	}

}