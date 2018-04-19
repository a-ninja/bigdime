/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.integration.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.bigdime.hbase.client.DataDeletionSpecification;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.admin.TableCreationSpecification;
import io.bigdime.hbase.client.admin.TableDeletionSpecification;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.hbase.common.HBaseConfigConstants;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@ContextConfiguration(locations = { "classpath:hbase-client-context/applicationContext.xml" })
public class HBaseClientTests extends AbstractTestNGSpringContextTests {

	@Value("${hbase.zookeeper.quorum}")
	private String hbaseZookeeperQuroum;
	@Value("${hbase.zookeeper.property.clientPort}")
	private String hbaseZookeeperPropertyClientPort;
	@Value("${zookeeper.znode.parent}")
	private String zookeeperZnodeParent;
	@Value("${hbase.connection.timeout}")
	private int hbaseConnectionTimeout;
	@Value("${hbase.test.user.data}")
	private String hbaseTestUserData;

	@Autowired
	HbaseManager hbaseManager;

	private Configuration config = null;

	private String tableName = "users_test10";
	private String columnFamilyOne = "attributes";
	private String columnQualifier = "id";

	private String tempID = "123471-12321";
//	private String tempID = "test.1448308085598";

	private JsonNode attributeData = null;

	@BeforeTest
	public void setup() {
		System.setProperty("env", "test");
	}

	@BeforeClass
	public void beforeClass() throws IOException {
		config = HBaseConfiguration.create();
		config.setInt(HBaseConfigConstants.HBASE_TIMEOUT,
				hbaseConnectionTimeout);
		config.set(HBaseConfigConstants.HBASE_ZOOKEEPER_CLIENT_PORT,
				hbaseZookeeperPropertyClientPort);
		config.set(HBaseConfigConstants.HBASE_ZOOKEEPER_QUORUM,
				hbaseZookeeperQuroum);
		config.set(HBaseConfigConstants.HBASE_ZOOKEEPER_PARENT,
				zookeeperZnodeParent);

		String fileLocation = System.getProperty("user.dir")
				+ hbaseTestUserData;
		String userDataJson = FileUtils
				.readFileToString(new File(fileLocation));
		ObjectMapper mapper = new ObjectMapper();
		JsonNode testData = mapper.readTree(userDataJson.toString());
		// JsonNode tableData = testData.get(tableName);
		JsonNode tableData = testData.get(tableName);
		attributeData = tableData.get(columnFamilyOne);
	}

	@Test(priority = 1)
	public void testCreateTable() throws IOException, HBaseClientException {
		List<HColumnDescriptor> columFamilies = new ArrayList<HColumnDescriptor>();
		HColumnDescriptor descrpitor = new HColumnDescriptor(columnFamilyOne);
		columFamilies.add(descrpitor);
		TableCreationSpecification.Builder tableCreationSpecificationBuilder = new TableCreationSpecification.Builder();
		TableCreationSpecification tableCreationSpecification = tableCreationSpecificationBuilder
				.withTableName(tableName)
				.withColumnsFamilies(columFamilies).build();
		hbaseManager.createTable(tableCreationSpecification);
		Assert.assertTrue(checkTableExist(tableName));
	}

	public boolean checkTableExist(String tableName) throws IOException {
		HBaseAdmin hAdmin = new HBaseAdmin(config);
		boolean isTableCreated = false;
		isTableCreated = hAdmin.tableExists(tableName);
		hAdmin.close();
		return isTableCreated;
	}

	@Test(priority = 8)
	public void testDeleteTable() throws IOException, InterruptedException,
			HBaseClientException {
		Thread.sleep(3000);
		TableDeletionSpecification.Builder tableDeletionSpecificationBuilder = new TableDeletionSpecification.Builder();
		TableDeletionSpecification tableDeletionSpecification = tableDeletionSpecificationBuilder
				.withTableName(tableName).build();
		hbaseManager.deleteTable(tableDeletionSpecification);
		Assert.assertFalse(checkTableExist(tableName));
	}

	@Test(priority = 2)
	public void testInsertData() throws IOException, HBaseClientException {
		Preconditions.checkNotNull(attributeData);
		Put put = null;
		if (attributeData.isArray()) {
			for (JsonNode user : attributeData) {
				Iterator<Entry<String, JsonNode>> userData = user.fields();
				Preconditions.checkNotNull(user.get(columnQualifier)
						.asText());
				put = new Put(user.get(columnQualifier).asText()
						.getBytes(StandardCharsets.UTF_8));
				while (userData.hasNext()) {
					Entry<String, JsonNode> kv = userData.next();
					put.add(columnFamilyOne.getBytes(StandardCharsets.UTF_8), kv.getKey().getBytes(StandardCharsets.UTF_8),
							kv.getValue().asText().getBytes(StandardCharsets.UTF_8));
				}
			}
		}

		DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
		DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
				.withTableName(tableName).withtPut(put).build();
		hbaseManager.insertData(dataInsertionSpecification);
	}

	@Test(priority = 3)
	public void testInsertAllData() throws IOException, HBaseClientException {
		Preconditions.checkNotNull(attributeData);
		List<Put> list = new ArrayList<Put>();
		Put put = null;
		if (attributeData.isArray()) {
			for (JsonNode user : attributeData) {
				Iterator<Entry<String, JsonNode>> userData = user.fields();
				Preconditions.checkNotNull(user.get(columnQualifier)
						.asText());
				put = new Put(user.get(columnQualifier).asText()
						.getBytes(StandardCharsets.UTF_8));
				while (userData.hasNext()) {
					Entry<String, JsonNode> kv = userData.next();
					put.add(columnFamilyOne.getBytes(StandardCharsets.UTF_8), kv.getKey().getBytes(StandardCharsets.UTF_8),
							kv.getValue().asText().getBytes(StandardCharsets.UTF_8));
				}
				list.add(put);
			}
		}
		DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
		DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
				.withTableName(tableName).withtPuts(list).build();
		hbaseManager.insertData(dataInsertionSpecification);

	}

	@Test(priority = 4)
	public void testRetrieveData() throws IOException, HBaseClientException {
		Get get = new Get(tempID.getBytes(StandardCharsets.UTF_8));
		DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
		DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
				.withTableName(tableName).withGet(get).build();
		hbaseManager.retreiveData(dataRetrievalSpecification);
		Result result = hbaseManager.getResult();
		Assert.assertFalse(result.isEmpty());
	}
	
//	@Test(priority = 4)
//	public void testRetrieveDataThreadSafteyTest() throws IOException, HBaseClientException {
//		Get get = new Get(tempID.getBytes());
//		DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
//		DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
//				.withTableName(tableName).withGet(get).build();
//		for(int i=0;i<10000;i++){
//		hbaseManager.retreiveData(dataRetrievalSpecification);
//		Result result = hbaseManager.result;
//		Assert.assertFalse(result.isEmpty());
//		}
//	}

	@Test(priority = 5)
	public void testRetrieveAllData() throws IOException, HBaseClientException {
		Scan scan = new Scan();

		DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
		DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
				.withTableName(tableName).withScan(scan).build();
		hbaseManager.retreiveData(dataRetrievalSpecification);
		ResultScanner resultScanner = hbaseManager.getResultScanner();
		Result result = null;
		Iterator<Result> iterator = resultScanner.iterator();
		while (iterator.hasNext()) {
			result = iterator.next();
			Assert.assertFalse(result.isEmpty());
		}
	}

	@Test(priority = 6)
	public void testDeleteData() throws IOException, HBaseClientException {
		Delete delete = new Delete(tempID.getBytes(StandardCharsets.UTF_8));
		DataDeletionSpecification.Builder dataDeletionSpecificationBuilder = new DataDeletionSpecification.Builder();
		DataDeletionSpecification dataDeletionSpecification = dataDeletionSpecificationBuilder
				.withTableName(tableName).withDelete(delete).build();
		hbaseManager.deleteData(dataDeletionSpecification);

		Get get = new Get(tempID.getBytes(StandardCharsets.UTF_8));
		DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
		DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
				.withTableName(tableName).withGet(get).build();
		hbaseManager.retreiveData(dataRetrievalSpecification);

		Result result = hbaseManager.getResult();
		Assert.assertTrue(result.isEmpty());
	}

	@Test(priority = 7)
	public void testDeleteAllData() throws HBaseClientException, IOException {
		List<Delete> list = new ArrayList<Delete>();
		Delete delete = null;

		if (attributeData.isArray()) {
			for (JsonNode user : attributeData) {
				Preconditions.checkNotNull(user.get(columnQualifier)
						.asText());
				delete = new Delete(user.get(columnQualifier).asText()
						.getBytes(StandardCharsets.UTF_8));
				list.add(delete);
			}
		}

		DataDeletionSpecification.Builder dataDeletionSpecificationBuilder = new DataDeletionSpecification.Builder();
		DataDeletionSpecification dataDeletionSpecification = dataDeletionSpecificationBuilder
				.withTableName(tableName).withDeletes(list).build();
		hbaseManager.deleteData(dataDeletionSpecification);

	}

}