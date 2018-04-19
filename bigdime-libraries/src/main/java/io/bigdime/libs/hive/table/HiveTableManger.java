/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.table;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.bigdime.libs.hive.client.HiveClientProvider;
import io.bigdime.libs.hive.common.ColumnMetaDataUtil;
import io.bigdime.libs.hive.common.HiveConfigManager;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.libs.hive.metadata.TableMetaData;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.HCatTable.Type;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.springframework.util.Assert;

import com.google.common.base.Preconditions;
/**
 * 
 * @author mnamburi
 *
 */
public class HiveTableManger extends HiveConfigManager {
	boolean tableCreated = false;

	private static final char defaultChar = '\u0000';

	public HiveTableManger(Properties properties) {
		super(properties);
	}

	public static HiveTableManger getInstance(){
		return getInstance(null);
	}
	
	public static HiveTableManger getInstance(Properties properties){
		return new HiveTableManger(properties);
	}
	/**
	 * 
	 * @param tableSpecfication
	 * @throws HCatException
	 */
	public synchronized void createTable(TableSpecification tableSpecfication) throws HCatException{
		HCatClient client = null;
		HCatCreateTableDesc tableDescriptor;
		HCatTable htable = new HCatTable(tableSpecfication.databaseName, tableSpecfication.tableName);
		if(tableSpecfication.isExternal){
			Preconditions.checkNotNull(tableSpecfication.location,"Location cannot be null, if table is external");
			htable.tableType(Type.EXTERNAL_TABLE);
			htable.location(tableSpecfication.location);
		}
		if (tableSpecfication.fileFormat != null) {
			htable.fileFormat(tableSpecfication.fileFormat);
		}

		if (tableSpecfication.fieldsTerminatedBy != defaultChar) {
			htable.fieldsTerminatedBy(tableSpecfication.fieldsTerminatedBy);
		}

		if (tableSpecfication.linesTerminatedBy != defaultChar) {
			htable.linesTerminatedBy(tableSpecfication.linesTerminatedBy);
		}
		
		if(tableSpecfication.columns != null){
			List<HCatFieldSchema> hiveColumn = ColumnMetaDataUtil.prepareHiveColumn(tableSpecfication.columns);
			htable.cols(hiveColumn);
		}
		
		if(tableSpecfication.partitionColumns != null){
			List<HCatFieldSchema> partitionColumns = ColumnMetaDataUtil.prepareHiveColumn(tableSpecfication.partitionColumns);
			htable.partCols(partitionColumns);
		}
		
		HCatCreateTableDesc.Builder tableDescBuilder = HCatCreateTableDesc
				.create(htable).ifNotExists(true);
		tableDescriptor = tableDescBuilder.build();

		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			client.createTable(tableDescriptor);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
	/**
	 * This method check table is created in hive metastroe
	 * @param databaseName
	 * @param tableName
	 * @return true if table is created, otherwise return false
	 * @throws HCatException
	 */
	public synchronized boolean isTableCreated(String databaseName, String tableName) throws HCatException{
		HCatClient client = null;
		try {
			if(tableCreated)
				return tableCreated;
			client = HiveClientProvider.getHcatClient(hiveConf);
			HCatTable hcatTable = client.getTable(databaseName, tableName);
			Assert.hasText(hcatTable.getTableName(), "table is null");
			tableCreated = true;
		}finally{
			HiveClientProvider.closeClient(client);
		}		
		return tableCreated;
	}
	
	/**
	 * this method ensure the table is deleted from hive metastore, by default the isExist flag is true.
	 * @param tableSpecfication
	 * @throws HCatException
	 */
	public void dropTable(String databaseName,String tableName) throws HCatException{
		HCatClient client = null;
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			client.dropTable(databaseName, tableName, Boolean.TRUE);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
	/**
	 * 
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public TableMetaData getTableMetaData(String databaseName,String tableName) throws HCatException{
		HCatClient client = null;
		TableMetaData tableMetaData;
		HCatTable hcatUserTable = null;
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			hcatUserTable = client.getTable(databaseName, tableName);
			tableMetaData = new TableMetaData(hcatUserTable);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
		return tableMetaData;
	}
	
	/**
	 * Retrieve the data from HDFS block to compute the values.
	 * @param databaseName
	 * @param tableName
	 * @param filterCol
	 * @param configuration
	 * @return
	 * @throws HCatException
	 */
	public ReaderContext readData(String databaseName,
			String tableName, String filter, Map<String, String> configuration)
			throws HCatException {
		ReadEntity.Builder builder = new ReadEntity.Builder();
		ReadEntity entity = null;
		
		if(filter != null && !filter.isEmpty())
			entity = builder.withDatabase(databaseName).withTable(tableName).withFilter(filter).build();
		else
			entity = builder.withDatabase(databaseName).withTable(tableName).build();


		configuration.put(HiveConf.ConfVars.METASTOREURIS.toString(),configuration.get(HiveClientConstants.HIVE_METASTORE_URI));

		HCatReader reader = DataTransferFactory.getHCatReader(entity,
				configuration);
		
		ReaderContext context = null;
		try {
			context = reader.prepareRead();
		} catch (Throwable ex) {
			throw ex;
		}
		return context;
	}
}
