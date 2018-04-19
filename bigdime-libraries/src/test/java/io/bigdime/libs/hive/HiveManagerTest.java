/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive;

import io.bigdime.common.testutils.factory.EmbeddedHiveServer;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.database.DatabaseSpecification;
import io.bigdime.libs.hive.database.HiveDBManger;
import io.bigdime.libs.hive.metadata.PartitionMetaData;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.partition.HivePartitionManger;
import io.bigdime.libs.hive.partition.PartitionSpecification;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.libs.hive.table.TableSpecification;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HiveManagerTest {
	HiveConf   hcatConf  = null;
	Properties props = new Properties();
	EmbeddedHiveServer embeddedHiveServer = null;

	HiveDBManger hiveDBManager = null;
	HiveTableManger hiveTableManager = null;
	HivePartitionManger hivePartitionManager = null;

	private static final String tableName  = "mockTable";
	private  static final String dbName = "mockdb";
	private String partitionLocation = "/partition/20150101";
	private String tableLocation = "/hive/table";
	private String dbLocation = "/hive/db";
	private String testDataDirectory = null;
	private String BD_TEST = "bigDimeTest";

	private HashMap<String,String> partitionMap = new HashMap<String,String>();
	
	@BeforeClass
	public void before() throws InterruptedException, IOException{
		embeddedHiveServer = EmbeddedHiveServer.getInstance();
		embeddedHiveServer.startMetaStore();
		props.put(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + embeddedHiveServer.getHivePort());
		//		System.out.println("thrift://localhost:" + embeddedHiveServer.getHivePort());
		//props.put(HiveConf.ConfVars.METASTOREURIS, "thrift://sandbox.hortonworks.com:9083");		
		props.put(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
		props.put(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
		props.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, Boolean.FALSE.toString());
		hiveDBManager = HiveDBManger.getInstance(props);
		hiveTableManager = HiveTableManger.getInstance(props);
		hivePartitionManager = HivePartitionManger.getInstance(props);

		partitionMap.put("dt", "20150101");
		
		testDataDirectory = FileUtils.getUserDirectoryPath()  + File.separator + BD_TEST + File.separator + BD_TEST + "-" + System.currentTimeMillis();
		partitionLocation = testDataDirectory + "/partition/20150101";
		tableLocation = testDataDirectory +  "/hive/table";
		dbLocation = testDataDirectory +  "/hive/db";	
		  
	}

	@Test
	public void testHiveManagerCreate() throws HCatException{
		createDatabase();
		createTable();
		verifyTable();
		createPartition();
		verifyPartition();
	}
	@Test(expectedExceptions=HCatException.class,dependsOnMethods={"testHiveManagerCreate"})
	public void testHiveManagerDrop() throws HCatException{
		dropPartition();
		hivePartitionManager.getPartitionMetadata(dbName, tableName, partitionMap);
		hiveTableManager.dropTable(dbName,tableName);
		hiveTableManager.getTableMetaData(dbName, tableName);
		boolean dbCreated = hiveDBManager.isDatabaseCreated(dbName);
		Assert.assertEquals(true, dbCreated);
		hiveDBManager.dropDatabase(dbName);
		hiveDBManager.isDatabaseCreated(dbName);
		Assert.assertEquals(false, dbCreated);
	}
	
	@AfterTest
	public void cleanup(){
//		try {
//			hiveTableManager.dropTable(dbName,tableName);
//			hiveDBManager.dropDatabase(dbName);
//		} catch (HCatException e) {
//			e.printStackTrace();
//		}
		FileUtils.deleteQuietly(new File(testDataDirectory));
	}
	//@Test
	public void createDatabase() throws HCatException{
		HiveDBManger hiveDBManager = HiveDBManger.getInstance(props);
		DatabaseSpecification.Builder databaseSpecificationBuilder = new DatabaseSpecification.Builder(dbName);
		DatabaseSpecification  databaseSpecification = databaseSpecificationBuilder.location(dbLocation)
				.comment("testcomment")
				.build();
		hiveDBManager.createDatabase(databaseSpecification);
	}
	private void createTable() throws HCatException{
		TableSpecification.Builder tableSpecBuilder = new TableSpecification.Builder(dbName, tableName);
		List<Column> columns = new ArrayList<Column>();
		Column column = new Column("col1", "String", "col1 comment");
		columns.add(column);
		column = new Column("col2", "String", "col2 comment");
		columns.add(column);

		List<Column> partitionColumns = new ArrayList<Column>();
		Column partitionColumn = new Column("dt", "String", "dt comment");
		partitionColumns.add(partitionColumn);
		TableSpecification  tableSpec = tableSpecBuilder.externalTableLocation(tableLocation)
				.columns(columns)
				.fieldsTerminatedBy('\001')
				.linesTerminatedBy('\001').partitionColumns(partitionColumns).fileFormat("Text").build();
		hiveTableManager.createTable(tableSpec);
	}

	private void verifyTable() throws HCatException{
		TableMetaData tableMetadata = hiveTableManager.getTableMetaData(dbName, tableName);
		Assert.assertEquals(tableMetadata.getName(), tableName.toLowerCase());
		Assert.assertEquals(tableMetadata.getDatabaseName(), dbName.toLowerCase());
		Assert.assertEquals(2, tableMetadata.getColumns().size());
		Assert.assertEquals(1, tableMetadata.getPartitionColumns().size());
		Assert.assertNotNull(tableMetadata.getTable());
		Assert.assertNotEquals(tableMetadata.getInputFileFormat(), "Text");
		Assert.assertNotNull(tableMetadata.getSerdeLib());
	}
	private void verifyPartition() throws HCatException{
		PartitionMetaData partitionMetaData = hivePartitionManager.getPartitionMetadata(dbName, tableName, partitionMap);
		Assert.assertEquals(partitionMetaData.getTableName(), tableName.toLowerCase());
		Assert.assertEquals(partitionMetaData.getDatabaseName(), dbName.toLowerCase());
		Assert.assertEquals(2, partitionMetaData.getColumns().size());
		Assert.assertEquals(1, partitionMetaData.getValues().size());
		Assert.assertEquals(partitionMetaData.getLocation(),"file:"+partitionLocation);
	}
	
	private void createPartition() throws HCatException{
		PartitionSpecification.Builder partitionSpecificationBuilder = new PartitionSpecification.Builder(dbName, tableName);

		PartitionSpecification partitionSpecification = partitionSpecificationBuilder
				.location(partitionLocation)
				.partitionColumns(partitionMap)
				.build();
		hivePartitionManager.addPartition(partitionSpecification);	
//		hivePartitionManager.dropPartition(partitionSpecification);
//		hcatPartition = hivePartitionManager.getPartition(dbName, tableName, partitionMap);
//		Assert.assertNull(hcatPartition);
	}
	private void dropPartition() throws HCatException{
		PartitionSpecification.Builder partitionSpecificationBuilder = new PartitionSpecification.Builder(dbName, tableName);

		PartitionSpecification partitionSpecification = partitionSpecificationBuilder
				.partitionColumns(partitionMap)
				.build();			
		hivePartitionManager.dropPartition(partitionSpecification);
	}
}
