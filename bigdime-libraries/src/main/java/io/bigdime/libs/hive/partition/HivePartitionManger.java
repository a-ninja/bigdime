/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.partition;


import java.util.Map;
import java.util.Properties;

import io.bigdime.libs.hive.client.HiveClientProvider;
import io.bigdime.libs.hive.common.HiveConfigManager;
import io.bigdime.libs.hive.metadata.PartitionMetaData;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.HCatTable.Type;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 * @author mnamburi
 *
 */
public class HivePartitionManger extends HiveConfigManager {
	private static final Logger logger = LoggerFactory.getLogger(HivePartitionManger.class);

	public HivePartitionManger(Properties properties) {
		super(properties);
	}
	
	public static HivePartitionManger getInstance(){
		return getInstance(null);
	}
	
	public static HivePartitionManger getInstance(Properties properties){
		return new HivePartitionManger(properties);
	}	
	/**
	 * 
	 * @param partitionSpecification
	 * @throws HCatException
	 */
	public boolean addPartition(PartitionSpecification partitionSpecification) throws HCatException{
		HCatClient client = HiveClientProvider.getHcatClient(hiveConf);
		boolean isSuccess = false;
		try {
			HCatTable htable = client.getTable(partitionSpecification.databaseName, partitionSpecification.tableName);
			if(Type.EXTERNAL_TABLE.name().equals(htable.getTabletype())){
				Preconditions.checkNotNull(partitionSpecification.location,"Location cannot be null, if table is external");
				htable.location(partitionSpecification.location);
			}
			HCatPartition hcatPartition = new HCatPartition(htable,partitionSpecification.partitionColumns,partitionSpecification.location);
			HCatAddPartitionDesc.Builder builder = HCatAddPartitionDesc.create(hcatPartition); 
			HCatAddPartitionDesc hcatPartitionDesc = builder.build();
			client.addPartition(hcatPartitionDesc);
		} catch (HCatException e) {
			if (e.getCause() instanceof AlreadyExistsException) {
				logger.warn("_message=\"Hive Partition already exists \" messageType = WARN "
						+ "databasse = {} tableName = {} partition = {}",
						partitionSpecification.databaseName, partitionSpecification.tableName, partitionSpecification.partitionColumns);
				isSuccess = true;
			} else {
				logger.warn("_message=\"Hive Partition Exception \" messageType = WARN "
						+ "databasse = {} tableName = {} partition = {} exception = {}",
						partitionSpecification.databaseName, partitionSpecification.tableName, partitionSpecification.partitionColumns, e);
				throw e;
			}
		}finally{
			HiveClientProvider.closeClient(client);
		}
		return isSuccess;

	}
	/**
	 * 
	 * @param partitionSpecification
	 * @throws HCatException
	 */
	public void dropPartition(PartitionSpecification partitionSpecification) throws HCatException{
		HCatClient client = HiveClientProvider.getHcatClient(hiveConf);
		try {
			client.dropPartitions(partitionSpecification.databaseName,
					partitionSpecification.tableName,
					partitionSpecification.partitionColumns,
					Boolean.TRUE);			
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}

	/**
	 * 
	 * @param dbName
	 * @param tableName
	 * @param partitionSpec
	 * @return
	 * @throws HCatException
	 */
	public PartitionMetaData getPartitionMetadata(String dbName,String tableName,Map<String, String> partitionSpec) throws HCatException{
		HCatClient client = HiveClientProvider.getHcatClient(hiveConf);
		PartitionMetaData partitionMetaData = null;
		try {
			HCatPartition hcatPartition = client.getPartition(dbName,tableName,partitionSpec);
			partitionMetaData = new PartitionMetaData(hcatPartition);
			return partitionMetaData;
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
}
