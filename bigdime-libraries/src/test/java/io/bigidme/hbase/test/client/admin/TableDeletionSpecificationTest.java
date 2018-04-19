/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client.admin;

import io.bigdime.hbase.client.admin.TableDeletionSpecification;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */

public class TableDeletionSpecificationTest extends PowerMockTestCase{

	TableDeletionSpecification tableDeletionSpecification;
	TableDeletionSpecification.Builder tableDeletionSpecificationBuilder;
	@BeforeClass
	 public void init()		{
		tableDeletionSpecificationBuilder=new TableDeletionSpecification.Builder();
	}
	
	@BeforeTest
	public void setup() {
		System.setProperty("env",TEST);	
	}
	
	@Test
	public void builderDeleteTableTest(){
		tableDeletionSpecification=tableDeletionSpecificationBuilder.withTableName(TEST).build();	
        Assert.assertEquals(tableDeletionSpecification.getTableName(),TEST);
	}
}
