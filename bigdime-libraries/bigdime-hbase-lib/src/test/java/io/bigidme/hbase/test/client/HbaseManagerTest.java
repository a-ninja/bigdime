/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client;

import com.google.common.base.Preconditions;
import io.bigdime.hbase.client.DataDeletionSpecification;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.admin.TableCreationSpecification;
import io.bigdime.hbase.client.admin.TableDeletionSpecification;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.hbase.common.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
@PrepareForTest({ConnectionFactory.class,HBaseConfiguration.class,HbaseManager.class,Preconditions.class})
public class HbaseManagerTest extends PowerMockTestCase{
//	private static final Logger logger = LoggerFactory.getLogger(HbaseManagerTest.class);
	HbaseManager hbaseManager;
	Configuration configuration;
	@BeforeClass
	public void init(){
		 hbaseManager=new HbaseManager();
		 String hbaseZookeeperQuroum="hbaseZookeeperQuroum";
		 String hbaseZookeeperPropertyClientPort="123";
		 String zookeeperZnodeParent= "zookeeperZnodeParent";
		 String hbaseConnectionTimeout="123";		
		 configuration=Mockito.mock(Configuration.class);
		 PowerMockito.mockStatic(HBaseConfiguration.class);
		 PowerMockito.when(HBaseConfiguration.create()).thenReturn(configuration);
		 ReflectionTestUtils.setField(hbaseManager, "hbaseZookeeperQuroum", hbaseZookeeperQuroum);
		 ReflectionTestUtils.setField(hbaseManager,"hbaseZookeeperPropertyClientPort",hbaseZookeeperPropertyClientPort);
		 ReflectionTestUtils.setField(hbaseManager,"zookeeperZnodeParent",zookeeperZnodeParent);
		 ReflectionTestUtils.setField(hbaseManager,"hbaseConnectionTimeout",hbaseConnectionTimeout);
		 ReflectionTestUtils.setField(hbaseManager,"configuration",configuration);
		 hbaseManager.init();
	}
	@BeforeTest
	public void setup() {
		System.setProperty("env",TEST);	
	}
//	@Test
//	public void initTest() throws Exception{
//		String hbaseZookeeperQuroum="hbaseZookeeperQuroum";
//		String hbaseZookeeperPropertyClientPort="123";
//		String zookeeperZnodeParent= "zookeeperZnodeParent";
//		String hbaseConnectionTimeout="123";		
//		Configuration configuration=Mockito.mock(Configuration.class);
//		PowerMockito.mockStatic(HBaseConfiguration.class);
//		PowerMockito.when(HBaseConfiguration.create()).thenReturn(configuration);
////      Mockito.doNothing().when(configuration).set(Mockito.any(String.class),Mockito.any(String.class));
//		ReflectionTestUtils.setField(hbaseManager, "hbaseZookeeperQuroum", hbaseZookeeperQuroum);
//		ReflectionTestUtils.setField(hbaseManager,"hbaseZookeeperPropertyClientPort",hbaseZookeeperPropertyClientPort);
//		ReflectionTestUtils.setField(hbaseManager,"zookeeperZnodeParent",zookeeperZnodeParent);
//		ReflectionTestUtils.setField(hbaseManager,"hbaseConnectionTimeout",hbaseConnectionTimeout);
//		ReflectionTestUtils.setField(hbaseManager,"configuration",configuration);
//		hbaseManager.init();
//	}
	
	@Test(priority=1)
	public void retreiveDataGetTest() throws Exception{
		DataRetrievalSpecification dataRetrievalSpecification;
		DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder=new DataRetrievalSpecification.Builder();
		Get get=new Get(TEST.getBytes(StandardCharsets.UTF_8));
		dataRetrievalSpecification=dataRetrievalSpecificationBuilder.withTableName(TEST).withGet(get).build();
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		PowerMockito.mockStatic(ConnectionFactory.class);
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		Result resultMock=Mockito.mock(Result.class);
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenReturn(hTable);
		Mockito.when(hTable.get((Get) Mockito.any())).thenReturn(resultMock);
		hbaseManager.retreiveData(dataRetrievalSpecification);
		Assert.assertNull(hbaseManager.getResultScanner());
	}
	
	
	@Test(priority=2)
	public void retreiveDataScanTest() throws Exception{
		DataRetrievalSpecification dataRetrievalSpecification;
		DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder=new DataRetrievalSpecification.Builder();
		Scan scan=new Scan(TEST.getBytes(StandardCharsets.UTF_8));
		dataRetrievalSpecification=dataRetrievalSpecificationBuilder.withTableName(TEST).withScan(scan).build();
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		PowerMockito.mockStatic(ConnectionFactory.class);
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		ResultScanner resultScannerMock=Mockito.mock(ResultScanner.class);
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenReturn(hTable);
		Mockito.when(hTable.getScanner((Scan) Mockito.any())).thenReturn(resultScannerMock);
		hbaseManager.retreiveData(dataRetrievalSpecification);
		Assert.assertNotNull(hbaseManager.getResultScanner());
		
	}
	
	@Test (priority=3)
	public void insertDataPutsTest() throws Exception{
		DataInsertionSpecification dataInsertionSpecification;
		Put put =new Put(TEST.getBytes(StandardCharsets.UTF_8));
		List<Put> puts=new ArrayList<Put>();
		puts.add(put);
		PowerMockito.mockStatic(ConnectionFactory.class);
		DataInsertionSpecification.Builder dataInsertionSpecificationBuilder=new DataInsertionSpecification.Builder();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		dataInsertionSpecification=dataInsertionSpecificationBuilder.withTableName(TEST).withtPuts(puts).build();
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenReturn(hTable);
        hbaseManager.insertData(dataInsertionSpecification);
        Assert.assertNotNull(configuration);
	 
	}
	
	@Test(expectedExceptions=NullPointerException.class,priority=4)
	public void insertDataPutsNullPointerExceptionTest() throws Exception{
		DataInsertionSpecification dataInsertionSpecification;
		Put put =new Put(TEST.getBytes(StandardCharsets.UTF_8));
		List<Put> puts=new ArrayList<Put>();
		puts.add(put);
		PowerMockito.mockStatic(ConnectionFactory.class);
		DataInsertionSpecification.Builder dataInsertionSpecificationBuilder=new DataInsertionSpecification.Builder();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		dataInsertionSpecification=dataInsertionSpecificationBuilder.withTableName(null).withtPuts(puts).build();
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenReturn(hTable);
	    hbaseManager.insertData(dataInsertionSpecification);
	}
	
	@Test(expectedExceptions=IOException.class,priority=5)
	public void insertDataPutsIOExceptionTest() throws Exception{
		DataInsertionSpecification dataInsertionSpecification;
		Put put =new Put(TEST.getBytes(StandardCharsets.UTF_8));
		List<Put> puts=new ArrayList<Put>();
		puts.add(put);
		PowerMockito.mockStatic(Preconditions.class);
		Mockito.when(Preconditions.checkNotNull(Mockito.anyString())).thenReturn(TEST);
		Preconditions.checkNotNull(Mockito.anyString());
		PowerMockito.mockStatic(ConnectionFactory.class);
		DataInsertionSpecification.Builder dataInsertionSpecificationBuilder=new DataInsertionSpecification.Builder();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		dataInsertionSpecification=dataInsertionSpecificationBuilder.withTableName(null).withtPuts(puts).build();
		Mockito.when(hConnectionMock.getTable(Mockito.any(String.class))).thenThrow(IOException.class);
		hbaseManager.insertData(dataInsertionSpecification);
	}
	
	@Test(expectedExceptions=HBaseClientException.class,priority=5)
	public void insertDataPutsHBaseClientExceptionTest() throws Exception{
		DataInsertionSpecification dataInsertionSpecification;
		Put put =new Put(TEST.getBytes(StandardCharsets.UTF_8));
		List<Put> puts=new ArrayList<Put>();
		puts.add(put);
		PowerMockito.mockStatic(Preconditions.class);
		Mockito.when(Preconditions.checkNotNull(Mockito.anyString())).thenReturn(TEST);
		Preconditions.checkNotNull(Mockito.anyString());
		PowerMockito.mockStatic(ConnectionFactory.class);
		DataInsertionSpecification.Builder dataInsertionSpecificationBuilder=new DataInsertionSpecification.Builder();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		dataInsertionSpecification=dataInsertionSpecificationBuilder.withTableName(null).withtPuts(puts).build();
		Mockito.when(hConnectionMock.getTable(Mockito.any(String.class))).thenThrow(HBaseClientException.class);
		hbaseManager.insertData(dataInsertionSpecification);
	}
	
	
	@Test(priority=6)
	public void deleteDataTest() throws Exception{
		DataDeletionSpecification dataDeletionSpecification;
		Delete delete=new Delete(TEST.getBytes(StandardCharsets.UTF_8));
		DataDeletionSpecification.Builder dataDeletionSpecificationBuilder=new DataDeletionSpecification.Builder();
		dataDeletionSpecification=dataDeletionSpecificationBuilder.withTableName(TEST).withDelete(delete).build();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		PowerMockito.mockStatic(ConnectionFactory.class);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenReturn(hTable);
	    hbaseManager.deleteData(dataDeletionSpecification);
	    Assert.assertNotNull(configuration);
	}
	
	@Test(expectedExceptions=IOException.class,priority=7)
	public void deleteDataIOExceptionTest() throws Exception{
		DataDeletionSpecification dataDeletionSpecification;
		Delete delete=new Delete(TEST.getBytes(StandardCharsets.UTF_8));
		DataDeletionSpecification.Builder dataDeletionSpecificationBuilder=new DataDeletionSpecification.Builder();
		dataDeletionSpecification=dataDeletionSpecificationBuilder.withTableName(TEST).withDelete(delete).build();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		PowerMockito.mockStatic(ConnectionFactory.class);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenThrow(IOException.class);
	    hbaseManager.deleteData(dataDeletionSpecification);
	}
	
	@Test(expectedExceptions=HBaseClientException.class,priority=7)
	public void deleteDataHBaseClientExceptionTest() throws Exception{
		DataDeletionSpecification dataDeletionSpecification;
		Delete delete=new Delete(TEST.getBytes(StandardCharsets.UTF_8));
		DataDeletionSpecification.Builder dataDeletionSpecificationBuilder=new DataDeletionSpecification.Builder();
		dataDeletionSpecification=dataDeletionSpecificationBuilder.withTableName(TEST).withDelete(delete).build();
		HConnection hConnectionMock=Mockito.mock(HConnection.class);
		HTableInterface hTable =Mockito.mock(HTableInterface.class);
		PowerMockito.mockStatic(ConnectionFactory.class);
		Mockito.when(ConnectionFactory.getInstanceofHConnection((Configuration) Mockito.any())).thenReturn(hConnectionMock);
		Configuration configuration=Mockito.mock(Configuration.class);
		PowerMockito.whenNew(Configuration.class).withArguments((Configuration)Mockito.any()).thenReturn(configuration);
		Mockito.when(hConnectionMock.getTable(Mockito.anyString())).thenThrow(HBaseClientException.class);
	    hbaseManager.deleteData(dataDeletionSpecification);
	}

	@Test(priority=8)
	public void createTableTest() throws Exception{	
		TableCreationSpecification tableCreationSpecification;
		TableCreationSpecification.Builder tableCreationSpecificationBuilder=new TableCreationSpecification.Builder();
		HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(TEST);
		List<HColumnDescriptor> list =new ArrayList<HColumnDescriptor>();
		list.add(hColumnDescriptor);
		tableCreationSpecification=tableCreationSpecificationBuilder.withTableName(TEST).withColumnsFamilies(list).build();
        HBaseAdmin hBaseAdmin=Mockito.mock(HBaseAdmin.class);
		PowerMockito.whenNew(HBaseAdmin.class).withAnyArguments().thenReturn(hBaseAdmin);
		hbaseManager.createTable(tableCreationSpecification);
		 Assert.assertNotNull(configuration);
	}
	
	@Test(priority=9)
	public void deleteTableTest() throws Exception{
		TableDeletionSpecification tableDeletionSpecification;
		TableDeletionSpecification.Builder tableDeletionSpecificationBuilder=new TableDeletionSpecification.Builder();
		tableDeletionSpecification=tableDeletionSpecificationBuilder.withTableName(TEST).build();
		HBaseAdmin hBaseAdmin=Mockito.mock(HBaseAdmin.class);
		PowerMockito.whenNew(HBaseAdmin.class).withAnyArguments().thenReturn(hBaseAdmin);
		Mockito.doNothing().when(hBaseAdmin).disableTable(Mockito.anyString());
	    hbaseManager.deleteTable(tableDeletionSpecification);
	    Assert.assertNotNull(configuration);
	}
}
