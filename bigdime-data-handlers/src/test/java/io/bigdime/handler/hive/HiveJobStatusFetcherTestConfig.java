package io.bigdime.handler.hive;

import io.bigdime.libs.hdfs.WebHdfsReader;
import io.bigdime.libs.hdfs.WebhdfsFacade;
import io.bigdime.libs.hive.job.HiveJobStatusFetcher;
import io.bigdime.libs.hive.job.JobClientFactory;
import org.apache.hadoop.mapred.JobClient;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.Properties;

@Configuration
public class HiveJobStatusFetcherTestConfig {
    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() throws Exception {
        final PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        Properties properties = new Properties();
        properties.setProperty("yarn.site.xml.path", HiveJobStatusFetcherTestConfig.class.getClassLoader().getResource("yarn-site.xml").getFile());
        properties.setProperty("hive.jdbc.user.name", "unit-username");
        properties.setProperty("hive.jdbc.secret", "unit-secret");
        properties.setProperty("hdfs_hosts", "unit-hdfs_hosts");
        properties.setProperty("hdfs_port", "0");
        properties.setProperty("hdfs_user", "unit-hdfs_user");
        properties.setProperty("mapreduce.framework.name", "unit");
        properties.setProperty("hadoop.security.authentication", "simple");

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

    @Bean
    public HiveJobOutputFileValidator getHiveJobOutputFileValidator() {
        return Mockito.mock(HiveJobOutputFileValidator.class);
    }

    @Bean
    public WebHdfsReader getWebHdfsReader() {
        return Mockito.mock(WebHdfsReader.class);
    }
    @Bean

    public WebhdfsFacade getWebHdfsFacade() {
        return Mockito.mock(WebhdfsFacade.class);
    }

    @Bean
    public JobClientFactory getJobClientFactory() {
        return Mockito.mock(JobClientFactory.class);
    }

}