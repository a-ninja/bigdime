package io.bigdime.handler.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.springframework.stereotype.Component;

@Component
public class JobClientFactory {
	public JobClient createJobClient(Configuration conf) throws IOException {
		return new JobClient(conf);
	}
}
