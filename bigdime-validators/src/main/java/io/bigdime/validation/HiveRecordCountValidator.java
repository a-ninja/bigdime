/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.validation.common.AbstractValidator;

/**
 * RecordCountValidator class is to perform whether hdfs record count from hive matches source record count or not
 * 
 * record count mismatches --- validation fail
 * otherwise, validation success
 * 
 * @author Rita Liu
 */

@Factory(id = "record_count_hive", type = HiveRecordCountValidator.class)
@Component
@Scope("prototype")
public class HiveRecordCountValidator implements Validator {

	private static final Logger logger = LoggerFactory.getLogger(HiveRecordCountValidator.class);
	
	private String name;
	
	/**
	 * This validate method will get hdfs file raw checksum based on provided
	 * parameters from actionEvent, and get source file raw checksum based on
	 * source file path, then compare them
	 * 
	 * @param actionEvent
	 * @return true if both raw checksum are same, else return false and hdfs
	 *         file move to errorChecksum location
	 *
	 */
	private boolean isReadyToValidate(final ActionEvent actionEvent) {

		String sourceRecordCount = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_RECORD_COUNT);
		String validationReady = actionEvent.getHeaders().get(ActionEventHeaderConstants.VALIDATION_READY);
		
		if (validationReady != null && !validationReady.equalsIgnoreCase(Boolean.TRUE.toString())) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"processing HiveRecordCountValidator",
					"validation is skipped, not ready yet to validate , recordCount={}",
					sourceRecordCount);
			return false;
		}
		return true;
	}
	
	/**
	 * This validate method will compare Hdfs record count(get from Hive) and source record
	 * count
	 * 
	 * @param actionEvent
	 * @return true if Hdfs record count is same as source record count,
	 *         otherwise return false
	 * 
	 */

	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {
		
		ValidationResponse validationPassed = new ValidationResponse();
		if (!isReadyToValidate(actionEvent)) {
			validationPassed.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
			return validationPassed;
		}
		AbstractValidator commonCheckValidator = new AbstractValidator();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		String srcRCString = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_RECORD_COUNT);
		String hiveDBName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_DB_NAME);
		String hiveTableName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_TABLE_NAME);
		String hivePartitionNames = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES);
		String hivePartitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
		String hiveMetaStoreURL = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_METASTORE_URI);
		int sourceRecordCount = 0;

		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, srcRCString);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_METASTORE_URI, hiveMetaStoreURL);
		
		try {
			sourceRecordCount = Integer.parseInt(srcRCString);
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal source record count input while parsing string to integer");
			throw new NumberFormatException();
		}
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_DB_NAME, hiveDBName);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_TABLE_NAME, hiveTableName);
		Map<String, String> partitionColumnsMap = new LinkedHashMap<String, String>();
		if (StringUtils.isNotBlank(hivePartitionNames) && StringUtils.isNotBlank(hivePartitionValues)) {
			String[] partitionNameList = hivePartitionNames.split(DataConstants.COMMA);
			String[] partitionValueList = hivePartitionValues.split(DataConstants.COMMA);
			for (int i = 0; i < partitionNameList.length; i++) {
				partitionColumnsMap.put(partitionNameList[i].trim(), partitionValueList[i].trim());
			}
		}
		int hdfsRecordCount = 0;
		Properties props = new Properties();
		props.put(HiveConf.ConfVars.METASTOREURIS, hiveMetaStoreURL);
		long startTime = System.currentTimeMillis();
		try {
			hdfsRecordCount = getHdfsRecordCountFromHive(props, hiveDBName, hiveTableName, partitionColumnsMap, 
								getHAProperties(actionEvent));
		} catch (HCatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
					.getAdaptorName(), "HCatException",
					"Exception occurred while getting record count from hive, cause: "
							+ e.getCause());
			throw new DataValidationException(
					"Exception during getting record count from hive for table " +hiveTableName+ " in " +hiveDBName+ " database");
		}
		long endTime = System.currentTimeMillis();
		if (sourceRecordCount == hdfsRecordCount) {			
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Record count matches",
					"Hdfs record count({}) is same as source record count({}), hiveDBName: {}, hiveTableName: {}, partitionMap: {}", hdfsRecordCount, 
					sourceRecordCount, hiveDBName, hiveTableName, partitionColumnsMap);
			validationPassed.setValidationResult(ValidationResult.PASSED);
		} else {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Record count mismatches",
					"Hdfs record count({}) is not same as source record count({}), hiveDBName: {}, hiveTableName: {}, partitionMap: {}", hdfsRecordCount,
					sourceRecordCount, hiveDBName, hiveTableName, partitionColumnsMap);
			validationPassed.setValidationResult(ValidationResult.FAILED);
		}
		logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), 
				"Hive record count validation for table = "+ hiveTableName+" and partitionMap = "+ partitionColumnsMap, " finished in {} milliseconds", 
				 (endTime-startTime));
		return validationPassed;
	}

	/**
	 * This is for hdfs record count using Hive
	 * @param props
	 * @param databaseName
	 * @param tableName
	 * @param partitionMap
	 * @param configuration
	 * @return
	 * @throws HCatException
	 * @throws DataValidationException
	 */
	private int getHdfsRecordCountFromHive(Properties props, String databaseName, String tableName, Map<String, String> partitionMap,
			Map<String, String> configuration) throws HCatException, DataValidationException{
		int count = 0;
		String filterValue = null;
		if(partitionMap != null && !partitionMap.isEmpty()){
			filterValue = getFilterString(partitionMap);
		}
		HiveTableManger hiveTableManager = HiveTableManger.getInstance(props);
		if(!hiveTableManager.isTableCreated(databaseName, tableName)){
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
					.getAdaptorName(), "Hive table not exist", 
					"Hive table {} is not found in {} database", tableName, databaseName);
			throw new DataValidationException("Hive table " +tableName+ " is not found in " +databaseName);
		}else{
			ReaderContext cntxt = hiveTableManager.readData(
				databaseName,
				tableName,
				filterValue,
				configuration);
			
			for (int slaveNode = 0; slaveNode < cntxt.numSplits(); slaveNode++) {
				HCatReader reader = DataTransferFactory.getHCatReader(cntxt,
					slaveNode);
				Iterator<HCatRecord> records = reader.read();
				while (records.hasNext()) {
					count++;
					records.next();
				}
			}
		}
		return count;	
	}
	/**
	 * Return's Constructed string from the partition map.
	 * @param partitionSpec
	 * @return
	 */
	private static String getFilterString(Map<String, String> partitionSpec) {
		StringBuilder filter = new StringBuilder();
	    for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
			if(!entry.getKey().equalsIgnoreCase(ActionEventHeaderConstants.ENTITY_NAME)){
	      filter.append(entry.getKey()).append(DataConstants.EQUAL)
	      		.append(DataConstants.BACK_SLASH).append(entry.getValue())
	      		.append(DataConstants.BACK_SLASH).append(DataConstants.AND);
			}
	    }

	    int length = filter.toString().length();
	    if (length > 0)
	      filter.delete(length - DataConstants.AND.length(), length);
	    return filter.toString();
	  }
	
	private Map<String, String> getHAProperties(ActionEvent actionEvent) {
		Map<String, String> configMap = new HashMap<String, String>();
		String haEnabledString = actionEvent.getHeaders().get(HiveClientConstants.HA_ENABLED);
		Boolean haEnabled = Boolean.valueOf(haEnabledString);
		String hiveMetaStoreURL = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_METASTORE_URI);
		configMap.put(HiveClientConstants.HIVE_METASTORE_URI, hiveMetaStoreURL);
		if(haEnabled){
			String hiveProxyProvider = actionEvent.getHeaders().get(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER);
			String haServiceName = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_SERVICES);
			String dfsNameService = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_SERVICES);
			String dfsNameNode1 = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1);
			String dfsNameNode2 = actionEvent.getHeaders().get(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2);
		
			configMap.put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER.replace(HiveClientConstants.HA_SERVICE_NAME,
				haServiceName), hiveProxyProvider);
			configMap.put(HiveClientConstants.DFS_NAME_SERVICES, dfsNameService);
			configMap.put(HiveClientConstants.DFS_HA_NAMENODES.replace(HiveClientConstants.HA_SERVICE_NAME, haServiceName),
					HiveClientConstants.DFS_NAME_NODE_LIST);
			configMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1.replace(
					HiveClientConstants.HA_SERVICE_NAME, haServiceName), dfsNameNode1);
			configMap.put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2.replace(
					HiveClientConstants.HA_SERVICE_NAME, haServiceName), dfsNameNode2);
		}
		return configMap;
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
