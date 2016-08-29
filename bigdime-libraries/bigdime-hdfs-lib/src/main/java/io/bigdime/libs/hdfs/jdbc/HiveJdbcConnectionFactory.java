package io.bigdime.libs.hdfs.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class HiveJdbcConnectionFactory {
	private static final Logger logger = LoggerFactory.getLogger(HiveJdbcConnectionFactory.class);

	private static final String SEMI_COLON = ";";
	private static final String QUESTION_MARK = "?";

	public Connection getConnectionWithKerberosAuthentication(final String driverClassName, final String jdbcUrl,
			final String keytabUser, final String keytabPath, final Map<String, String> hiveConfigurations)
			throws IOException, SQLException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "Kerberos");
		return getConnectionWithKerberosAuthentication(driverClassName, jdbcUrl, conf, keytabUser, keytabPath,
				hiveConfigurations);
	}

	public Connection getConnectionWithKerberosAuthentication(final String driverClassName, final String jdbcUrl,
			final Configuration conf, final String keytabUser, final String keytabPath,
			final Map<String, String> hiveConfigurations) throws IOException, SQLException, ClassNotFoundException {
		return getConnection0(driverClassName, jdbcUrl, conf, keytabUser, keytabPath, hiveConfigurations);
	}

	private Connection getConnection0(final String driverClassName, final String jdbcUrl, final Configuration conf,
			final String keytabUser, final String keytabPath, final Map<String, String> hiveConfigurations)
			throws IOException, SQLException, ClassNotFoundException {

		String jdbcUrlWithConf = jdbcUrl;

		StringBuilder hiveConfVars = new StringBuilder();
		for (String key : hiveConfigurations.keySet()) {
			hiveConfVars.append(key).append("=").append(hiveConfigurations.get(key)).append(SEMI_COLON);
		}
		if (hiveConfVars.length() > 0) {
			jdbcUrlWithConf = jdbcUrl + QUESTION_MARK + hiveConfVars.toString();
		}

		logger.debug("connecting to db, using kerberos auth",
				"jdbcUrl=\"{}\" driverClassName=\"{}\" keytabUser=\"{}\" keytabPath=\"{}\"", jdbcUrlWithConf,
				driverClassName, keytabUser, keytabPath);

		BasicDataSource datasource = new BasicDataSource();
		datasource.setDriverClassName(driverClassName);
		datasource.setUrl(jdbcUrlWithConf);
		loginUserFromKeytab(conf, keytabUser, keytabPath);

		return datasource.getConnection();

	}

	private void loginUserFromKeytab(final Configuration conf, final String keytabUser, final String keytabPath)
			throws IOException {
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(keytabUser, keytabPath);

	}

	public Connection getConnection(final String driverClassName, final String jdbcUrl, final String username,
			final String password, final Map<String, String> hiveConfigurations) throws IOException, SQLException {
		return getConnection0(driverClassName, jdbcUrl, username, password, hiveConfigurations);
	}

	private Connection getConnection0(final String driverClassName, final String jdbcUrl, final String username,
			final String password, final Map<String, String> hiveConfigurations) throws IOException, SQLException {

		String jdbcUrlWithConf = jdbcUrl;

		StringBuilder hiveConfVars = new StringBuilder();
		for (String key : hiveConfigurations.keySet()) {
			hiveConfVars.append(key).append("=").append(hiveConfigurations.get(key)).append(SEMI_COLON);
		}
		if (hiveConfVars.length() > 0) {
			jdbcUrlWithConf = jdbcUrl + QUESTION_MARK + hiveConfVars.toString();
		}

		logger.debug("connecting to db, using username and password",
				"jdbcUrl=\"{}\" driverClassName=\"{}\" username=\"{}\" password=\"{}\"", jdbcUrlWithConf,
				driverClassName, username, "*****");

		BasicDataSource datasource = new BasicDataSource();
		datasource.setDriverClassName(driverClassName);
		datasource.setUrl(jdbcUrlWithConf);
		datasource.setUsername(username);
		datasource.setPassword(password);
		return datasource.getConnection();
	}
}
