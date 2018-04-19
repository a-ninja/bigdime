/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client;

import io.bigdime.hbase.client.DataInsertionSpecification;
import org.apache.hadoop.hbase.client.Put;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class DataInsertionSpecificationTest extends PowerMockTestCase{

	DataInsertionSpecification dataInsertionSpecification;
	DataInsertionSpecification.Builder dataInsertionSpecificationBuilder;
	
	@BeforeClass
	 public void init()		{
		dataInsertionSpecificationBuilder=new DataInsertionSpecification.Builder();
	}
	
	@BeforeTest
	public void setup() {
		System.setProperty("env",TEST);	
	}
	
	@Test
	public void builderPutsTest(){
		Put put=new Put(TEST.getBytes(StandardCharsets.UTF_8));
		put.setAttribute(TEST, TEST.getBytes(StandardCharsets.UTF_8));
		List<Put> list=new ArrayList<Put>();
		list.add(put);
		dataInsertionSpecification=dataInsertionSpecificationBuilder.withTableName(TEST).withtPuts(list).build();
		Assert.assertEquals( dataInsertionSpecification.getTableName(), TEST);
		for(Put putValue:dataInsertionSpecification.getPuts()){
		Assert.assertEquals(new String(putValue.getAttribute(TEST),StandardCharsets.UTF_8),TEST);	
		}
	}
}
