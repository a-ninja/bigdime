package io.bigdime.alert.multiple.impl.integration.test;

import java.io.IOException;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.multiple.impl.BigdimeHBaseLogger;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@ContextConfiguration(locations = { "classpath:META-INF/application-context-monitoring.xml" })
public class BigdimeHbaseLoggerTest  extends AbstractTestNGSpringContextTests{
	
	@Value("${hbase.zookeeper.quorum}")
	private String hbaseZookeeperQuroum;
	@Value("${hbase.zookeeper.property.clientPort}")
	private String hbaseZookeeperPropertyClientPort;
	@Value("${zookeeper.znode.parent}")
	private String zookeeperZnodeParent;
	@Value("${hbase.connection.timeout}")
	private int hbaseConnectionTimeout;
	@Autowired
	HbaseManager hbaseManager;

	@Test(priority = 1)
	public void testPut() throws IOException, HBaseClientException{
		Logger logger = LoggerFactory.getLogger(BigdimeHBaseLogger.class);
		logger.alert("test_source", ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER, "unit-message");
		logger.debug("test_source","unit-message", "unit-message");

	}

}
