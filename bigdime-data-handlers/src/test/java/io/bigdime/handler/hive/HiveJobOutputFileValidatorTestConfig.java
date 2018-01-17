package io.bigdime.handler.hive;

import java.util.Properties;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import io.bigdime.libs.hdfs.WebHdfsReader;

@Configuration
public class HiveJobOutputFileValidatorTestConfig {
	@Bean
	public static PropertySourcesPlaceholderConfigurer properties() throws Exception {
		final PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
		Properties properties = new Properties();

		properties.setProperty("yarn.site.xml.path", "yarn-site.xml");
		properties.setProperty("hive.jdbc.user.name", "unit-username");
		properties.setProperty("hive.jdbc.secret", "unit-secret");

		pspc.setProperties(properties);
		return pspc;
	}

	@Bean
	public HiveJobOutputFileValidator getHiveJobOutputFileValidator() {
		return new HiveJobOutputFileValidator();
	}

	@Bean
	public WebHdfsReader getWebHdfsReader() {
		return Mockito.mock(WebHdfsReader.class);
	}
}