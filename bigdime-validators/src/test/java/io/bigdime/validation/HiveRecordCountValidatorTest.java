/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.HiveRecordCountValidator;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.libs.hive.table.HiveTableManger;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.http.client.ClientProtocolException;
import org.testng.Assert;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@PrepareForTest(HiveTableManger.class)
public class HiveRecordCountValidatorTest extends PowerMockTestCase {

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateHiveMetaStoreURITest() throws DataValidationException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(HiveClientConstants.HIVE_METASTORE_URI, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hiveRecordCountValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullSrcRecordCountTest() throws DataValidationException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hiveRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = NumberFormatException.class)
	public void validateSrcRCParseToIntTest() throws DataValidationException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "src");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hiveRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveDBNameTest() throws DataValidationException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hiveRecordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveTableNameTest() throws DataValidationException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		hiveRecordCountValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void validateHiveTableNotCreated() throws DataValidationException, HCatException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(false);
		hiveRecordCountValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void validateGetHDFSRecordCountException() throws DataValidationException, HCatException{
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.doThrow(HCatException.class).when(mockHiveTableManager).isTableCreated(anyString(), anyString());	
		hiveRecordCountValidator.validate(mockActionEvent);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithoutPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(HiveClientConstants.HA_ENABLED, Boolean.FALSE.toString());
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(hiveRecordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithPartitionMatchTest()
			throws DataValidationException, ClientProtocolException, IOException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "0");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "20120201");
		mockMap.put(HiveClientConstants.HA_ENABLED, Boolean.FALSE.toString());

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(hiveRecordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.PASSED);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithHAEnabledNoPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = mockSeedProperties();
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(),anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(hiveRecordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test
	public void gettersAndSettersTest() {
		HiveRecordCountValidator hiveRecordCountValidator = new HiveRecordCountValidator();
		hiveRecordCountValidator.setName("testName");
		Assert.assertEquals(hiveRecordCountValidator.getName(), "testName");
	}
	public Map<String, String> mockSeedProperties(){
		Map<String, String> mockMap = new HashMap<>();
		mockMap.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		mockMap.put(ActionEventHeaderConstants.HIVE_PORT, "111");
		mockMap.put(HiveClientConstants.HA_ENABLED, Boolean.TRUE.toString());
		mockMap.put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER, "dfs proxy");
		mockMap.put(HiveClientConstants.HA_SERVICE_NAME, "haServiceName");
		mockMap.put(HiveClientConstants.DFS_NAME_SERVICES, "dfs service");
		mockMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1, "dfs namenode 1");
		mockMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2, "dfs namenode 2");		
		mockMap.put("hive.metastore.uris", "host:111");
		
		mockMap.put(ActionEventHeaderConstants.HIVE_DB_NAME, "db");
		mockMap.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "table");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "");
		mockMap.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "");
		
		mockMap.put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
		mockMap.put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
		return mockMap;
	}
}
