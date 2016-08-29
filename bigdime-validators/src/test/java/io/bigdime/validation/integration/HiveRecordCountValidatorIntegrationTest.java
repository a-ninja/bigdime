/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.validation.HiveRecordCountValidator;

public class HiveRecordCountValidatorIntegrationTest{
	
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHiveHostName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "");
    	hiveRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullPort() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "");
    	hiveRecordCountValidator.validate(actionEvent);
    }
  
    @Test(expectedExceptions = NumberFormatException.class)
    public void testParsePortStringToIntException() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "port");
    	hiveRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullSrcRecordCount() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, null);
    	hiveRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = NumberFormatException.class)
    public void testParseSrcRecordCountStringToInt() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "count");
    	hiveRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHiveDBName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
    	hiveRecordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHiveTableName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
    	hiveRecordCountValidator.validate(actionEvent);
    }
 
    @Test(expectedExceptions = DataValidationException.class)
    public void testNullHiveTableNotCreated() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "not_exist");
    	hiveRecordCountValidator.validate(actionEvent);
    }
    
    @Test
    public void testValidateRecordCountWithPartitionsDiff() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "234");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "two_test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "entityName, dt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "two, 20120218");
    	actionEvent.getHeaders().put(HiveClientConstants.HA_ENABLED, "false");
    	Assert.assertEquals(hiveRecordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
    
    @Test
    public void testValidateRecordCountWithPartitionsHaEnabledDiff() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "slcd000hen202.stubcorp.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "234");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "stbcat");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "tickets");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "dt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "20140213");
    	actionEvent.getHeaders().put(HiveClientConstants.HA_ENABLED, "true");
    	actionEvent.getHeaders().put(HiveClientConstants.HA_SERVICE_NAME, "hdfs-cluster");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_NAME_SERVICES, "hdfs-cluster");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1, "slcd000hnn201.stubcorp.com:8020");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2, "slcd000hnn203.stubcorp.com:8020");
    	Assert.assertEquals(hiveRecordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
    
    @Test
    public void testValidateRecordCountWithoutPartitions() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "3");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "one");
    	Assert.assertEquals(hiveRecordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
    }
    
	@Test
	public void testSettersAndGetters(){
		HiveRecordCountValidator hiveRecordCountValidator= new HiveRecordCountValidator();
		hiveRecordCountValidator.setName("testName");
		Assert.assertEquals(hiveRecordCountValidator.getName(), "testName");
	}

}
