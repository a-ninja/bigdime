/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client.admin;

import io.bigdime.hbase.client.admin.TableCreationSpecification;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class TableCreationSpecificationTest extends PowerMockTestCase{

	TableCreationSpecification tableCreationSpecification;
	TableCreationSpecification.Builder tableCreationSpecificationBuilder;
	
	
	@BeforeClass
	 public void init()		{
		tableCreationSpecificationBuilder=new TableCreationSpecification.Builder();
	}
	
	@BeforeTest
	public void setup() {
		System.setProperty("env",TEST);	
	}
	
	@Test
	public void builderCreateTableTest(){
		HColumnDescriptor descrpitor = new HColumnDescriptor(TEST);
        List<HColumnDescriptor> list =new ArrayList<HColumnDescriptor>();
        list.add(descrpitor);
		tableCreationSpecification=tableCreationSpecificationBuilder.withTableName(TEST).withColumnsFamilies(list).build();	    
	    Assert.assertEquals(tableCreationSpecification.getTableName(), TEST);
		for(HColumnDescriptor columFamily: tableCreationSpecification.getColumnsFamilies()){
			Assert.assertEquals( columFamily.getNameAsString(),TEST);
	   }
	}
}
