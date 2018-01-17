/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client;

import com.google.common.base.Preconditions;
import io.bigdime.hbase.client.admin.TableCreationSpecification;
import io.bigdime.hbase.client.admin.TableDeletionSpecification;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.hbase.common.ConnectionFactory;
import io.bigdime.hbase.common.HBaseConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
@Component
@Scope("prototype")
public class HbaseManager {
	private static final Logger logger = LoggerFactory
			.getLogger(HbaseManager.class);
	@Value("${hbase.zookeeper.quorum}")
	private String hbaseZookeeperQuroum;
	@Value("${hbase.zookeeper.property.clientPort}")
	private String hbaseZookeeperPropertyClientPort;
	@Value("${zookeeper.znode.parent}")
	private String zookeeperZnodeParent;
	@Value("${hbase.connection.timeout}")
	private String hbaseConnectionTimeout;
	private Configuration configuration;

	private ResultScanner resultScanner = null;
	private Result result = null;
	private static final String appName = "BIGDIME-HBASE-LIB";

	
	public ResultScanner getResultScanner() {
		return resultScanner;
	}

	public Result getResult() {
		return result;
	}

	@PostConstruct
	public void init() {
		configuration = HBaseConfiguration.create();
		configuration.setInt(HBaseConfigConstants.HBASE_TIMEOUT,
				Integer.parseInt(hbaseConnectionTimeout));
		configuration.set(HBaseConfigConstants.HBASE_ZOOKEEPER_CLIENT_PORT,
				hbaseZookeeperPropertyClientPort);
		configuration.set(HBaseConfigConstants.HBASE_ZOOKEEPER_QUORUM,
				hbaseZookeeperQuroum);
		configuration.set(HBaseConfigConstants.HBASE_ZOOKEEPER_PARENT,
				zookeeperZnodeParent);
	}

	public void retreiveData(
			DataRetrievalSpecification dataRetrievalSpecification)
			throws HBaseClientException, IOException {
		logger.info(appName, "Validations",
				"Checking that dataRetrievalSpecification is not null");
		Preconditions.checkNotNull(dataRetrievalSpecification);
		String tableName = dataRetrievalSpecification.getTableName();
		Get get = dataRetrievalSpecification.getGet();
		Scan scan = dataRetrievalSpecification.getScan();
		logger.info(appName, "Validations",
				"Checking that tableName value is not null");
		Preconditions.checkNotNull(tableName);
		HConnection hConnection = getConnection(configuration);
		HTableInterface hTable = hConnection.getTable(tableName);
		if (get != null) {
			result = hTable.get(get);
		} else {
			resultScanner = hTable.getScanner(scan);
		}
		hTable.close();
		releaseHConnection(hConnection);

	}

	public void insertData(DataInsertionSpecification dataInsertionSpecification)
			throws IOException, HBaseClientException {
		Preconditions.checkNotNull(dataInsertionSpecification);
		String tableName = dataInsertionSpecification.getTableName();
		List<Put> puts = dataInsertionSpecification.getPuts();
//		logger.debug(appName, "Inserting the record", "tableName={} puts.size={}",
//				tableName, puts.size());
		Preconditions.checkNotNull(tableName);
		Preconditions.checkArgument(!puts.isEmpty());
		HConnection hConnection = getConnection(configuration);
		HTableInterface hTable = hConnection.getTable(tableName);
//		logger.debug(appName, "Inserting the record", "tableName={} records.size={}",
//				tableName, puts.size());
//		for (Put put : puts) {
//			logger.debug(appName, "Inserting the record", "tableName={} records={}",
//					tableName, put.toJSON());
//		}
		hTable.put(puts);
		hTable.close();
		releaseHConnection(hConnection);
	}

	public void deleteData(DataDeletionSpecification dataDeletionSpecification)
			throws IOException, HBaseClientException {
		Preconditions.checkNotNull(dataDeletionSpecification,"Checking that dataDeletionSpecification is not null");
		String tableName = dataDeletionSpecification.getTableName();
		List<Delete> deletes = dataDeletionSpecification.getDeletes();
		Preconditions.checkNotNull(tableName,"Checking that tableName value is not null");
		Preconditions.checkArgument(!deletes.isEmpty(),"Checking that deletes(the data to be deleted) is not Empty");
		HConnection hConnection = getConnection(configuration);
		HTableInterface hTable = hConnection.getTable(tableName);
		for (Delete delete : deletes) {
			logger.debug(appName, "Deleting the record", "tableName={} records={}",
					dataDeletionSpecification.getTableName(), delete.toJSON());
		}
		hTable.delete(deletes);
		hTable.close();
		releaseHConnection(hConnection);
	}

	public void createTable(
			TableCreationSpecification tableCreationSpecification)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		Preconditions.checkNotNull(tableCreationSpecification,"Checking that tableCreationSpecification is not null");
		String tableName = tableCreationSpecification.getTableName();
		List<HColumnDescriptor> columnFamilies = tableCreationSpecification
				.getColumnsFamilies();
		Preconditions.checkNotNull(tableName,"Checking that tableName value is not null");
		Preconditions.checkNotNull(columnFamilies,"Checking that columnFamily valuea are not null");
		HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
		Preconditions.checkNotNull(hbaseAdmin,"Checking that hbaseAdmin value is not null");
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor htableDescriptor = new HTableDescriptor(tn);
		Iterator<HColumnDescriptor> cfDetails = columnFamilies.iterator();
		HColumnDescriptor hColumnDescriptor = null;
		while (cfDetails.hasNext()) {
			hColumnDescriptor = cfDetails.next();
			htableDescriptor.addFamily(hColumnDescriptor);
		}
		hbaseAdmin.createTable(htableDescriptor);
		hbaseAdmin.close();
	}

	public void deleteTable(
			TableDeletionSpecification tableDeletionSpecification)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		Preconditions.checkNotNull(tableDeletionSpecification,"Checking that tableDeletionSpecification is not null");
		String tableName = tableDeletionSpecification.getTableName();
		Preconditions.checkNotNull(tableName,"Checking that tableName value is not null");
		HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
		Preconditions.checkNotNull(hbaseAdmin,"Checking that hbaseAdmin value is not null");
		hbaseAdmin.disableTable(tableName);
		hbaseAdmin.deleteTable(tableName);
		hbaseAdmin.close();
	}

	private HConnection getConnection(Configuration configuration)
			throws HBaseClientException, IOException {
		return ConnectionFactory.getInstanceofHConnection(configuration);
	}

	// private void releaseConnection(Configuration configuration){
	// ConnectionFactory.close(configuration);
	// }
	private void releaseHConnection(HConnection hConnection) throws IOException {
		ConnectionFactory.close(hConnection);
	}
}
