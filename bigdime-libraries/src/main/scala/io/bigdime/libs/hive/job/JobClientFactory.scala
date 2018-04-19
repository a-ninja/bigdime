package io.bigdime.libs.hive.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobClient
import org.springframework.stereotype.Component

import java.io.IOException

@Component
class JobClientFactory {
  @throws[IOException]
  def createJobClient(conf: Configuration): JobClient = {
    return new JobClient(conf)
  }
}
