package io.bigdime.impl.biz.dao;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;
public class PropertiesTest {
	
	private static final Logger logger = LoggerFactory.getLogger(PropertiesTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void propertiesGettersAndSettersTest(){
		Properties properties=new Properties();
		properties.setBrokers(TEST_STRING);
		properties.setOffsetdatadir(TEST_STRING);
		properties.setMessageSize(TEST_STRING);
		properties.setBatchSize(TEST_STRING);
		properties.setPrintstats(TEST_STRING);
		properties.setBasepath(TEST_STRING);
		properties.setBuffersize(TEST_STRING);
		properties.setPreservebasepath(TEST_STRING);
		properties.setPreserverelativepath(TEST_STRING);
		properties.setHostNames(TEST_STRING);
		properties.setPort(TEST_STRING);
		properties.setHdfsFileNamePrefix(TEST_STRING);
		properties.setHdfsFileNameExtension(TEST_STRING);
		properties.setHdfsPath(TEST_STRING);
		properties.setHdfsUser(TEST_STRING);
		properties.setHdfsOverwrite(TEST_STRING);
		properties.setHdfsPermissions(TEST_STRING);
		properties.setRegex(TEST_STRING);
		properties.setPartitionnames(TEST_STRING);
		properties.setHeadername(TEST_STRING);
		properties.setSchemaFileName(TEST_STRING);
		properties.setEntityName(TEST_STRING);
		properties.setValidationtype(TEST_STRING);
		properties.setChannelclass(TEST_STRING);
		properties.setDatepartitionname(TEST_STRING);
		properties.setDatepartitioninputformat(TEST_STRING);
		properties.setDatepartitionoutputformat(TEST_STRING);
		properties.setPartitionnamesoutputorder(TEST_STRING);
		properties.setHdfspathloweruppercase(TEST_STRING);
		properties.setPrintstatsdurationinseconds(TEST_STRING);
		properties.setChannelcapacity(TEST_STRING);
		properties.setHivemetastoreuris(TEST_STRING);
		
		Assert.assertEquals(TEST_STRING,properties.getBrokers());
		Assert.assertEquals(TEST_STRING,properties.getOffsetdatadir() );
		Assert.assertEquals(TEST_STRING,properties.getMessageSize());
		Assert.assertEquals(TEST_STRING,properties.getBatchSize());
		Assert.assertEquals(TEST_STRING,properties.getPrintstats());
		Assert.assertEquals(TEST_STRING,properties.getBasepath());
		Assert.assertEquals(TEST_STRING,properties.getBuffersize());
		Assert.assertEquals(TEST_STRING,properties.getPreservebasepath());
		Assert.assertEquals(TEST_STRING,properties.getPreserverelativepath());
		Assert.assertEquals(TEST_STRING,properties.getHostNames());
		Assert.assertEquals(TEST_STRING,properties.getPort());
		Assert.assertEquals(TEST_STRING,properties.getHdfsFileNamePrefix());
		Assert.assertEquals(TEST_STRING,properties.getHdfsFileNameExtension());
		Assert.assertEquals(TEST_STRING,properties.getHdfsPath());
		Assert.assertEquals(TEST_STRING,properties.getHdfsUser());
		Assert.assertEquals(TEST_STRING,properties.getHdfsOverwrite());
		Assert.assertEquals(TEST_STRING,properties.getHdfsPermissions());
		Assert.assertEquals(TEST_STRING,properties.getRegex());
		Assert.assertEquals(TEST_STRING,properties.getPartitionnames());
		Assert.assertEquals(TEST_STRING,properties.getHeadername());
		Assert.assertEquals(TEST_STRING,properties.getSchemaFileName());
		Assert.assertEquals(TEST_STRING,properties.getEntityName());
		Assert.assertEquals(TEST_STRING,properties.getValidationtype());
		Assert.assertEquals(TEST_STRING,properties.getChannelclass());
		Assert.assertEquals(TEST_STRING,properties.getDatepartitionname());
		Assert.assertEquals(TEST_STRING,properties.getDatepartitioninputformat());
		Assert.assertEquals(TEST_STRING,properties.getDatepartitionoutputformat());
		Assert.assertEquals(TEST_STRING,properties.getPartitionnamesoutputorder());
		Assert.assertEquals(TEST_STRING,properties.getHdfspathloweruppercase());
		Assert.assertEquals(TEST_STRING,properties.getPrintstatsdurationinseconds());
		Assert.assertEquals(TEST_STRING,properties.getChannelcapacity());
		Assert.assertEquals(TEST_STRING,properties.getHivemetastoreuris());
		
	}

}
