/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client;

import io.bigdime.hbase.client.DataRetrievalSpecification;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class DataRetrievalSpecificationTest extends PowerMockTestCase{

	DataRetrievalSpecification dataRetrievalSpecification;
	DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder;
	
	@BeforeClass
	 public void init()		{
		dataRetrievalSpecificationBuilder=new DataRetrievalSpecification.Builder();
	}
	@BeforeTest
	public void setup() {
		System.setProperty("env",TEST);	
	}
	
	@Test
	public void builderGetTest(){
		
		Get get=new Get(TEST.getBytes(StandardCharsets.UTF_8));
		get.setAttribute(TEST, TEST.getBytes(StandardCharsets.UTF_8));
		dataRetrievalSpecification=dataRetrievalSpecificationBuilder.withTableName(TEST).withGet(get).build();
		Assert.assertEquals(dataRetrievalSpecification.getTableName(),TEST);
		Assert.assertEquals(new String(dataRetrievalSpecification.getGet().getAttribute(TEST),StandardCharsets.UTF_8),TEST);
	}
	 @Test
	 public void builderScanTest(){
			Scan scan=new Scan(TEST.getBytes(StandardCharsets.UTF_8));
			scan.setAttribute(TEST, TEST.getBytes(StandardCharsets.UTF_8));
			dataRetrievalSpecification=dataRetrievalSpecificationBuilder.withTableName(TEST).withScan(scan).build();
			Assert.assertEquals(dataRetrievalSpecification.getTableName(),TEST);
			Assert.assertEquals(new String(dataRetrievalSpecification.getScan().getAttribute(TEST),StandardCharsets.UTF_8),TEST);		 
	 }
	 
	
}
