package io.bigdime.handler.hive;

import java.util.Properties;

import org.apache.hadoop.mapred.JobClient;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
public class HiveJobStatusFetcherTestConfig {
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
	public HiveJobStatusFetcher getHiveJobStatusFetcher() {
		return new HiveJobStatusFetcher();
	}

	@Bean
	public JobClient getJobClient() {
		return Mockito.mock(JobClient.class);
	}
}