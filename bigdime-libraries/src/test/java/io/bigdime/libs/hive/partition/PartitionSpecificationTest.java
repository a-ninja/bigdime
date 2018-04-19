/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.partition;

import io.bigdime.libs.hive.partition.PartitionSpecification;
import io.bigdime.libs.hive.partition.PartitionSpecification.Builder;

import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PartitionSpecificationTest {
	@Test
	public void testSpecficationBuild(){
		HashMap<String,String> partitionMap = new HashMap<String,String>();
		partitionMap.put("dt", "20150101");
		Builder partitionSpecificationBuilder = new Builder("mockdb", "mockTable");
		PartitionSpecification partitionSpecification = partitionSpecificationBuilder
														.location("/partition/20150101")
														.partitionColumns(partitionMap)
														.build();
		Assert.assertEquals(partitionSpecification.location, "/partition/20150101");
		Assert.assertEquals(partitionSpecification.databaseName, "mockdb");
		Assert.assertEquals(partitionSpecification.tableName, "mockTable");
		Assert.assertEquals(partitionSpecification.partitionColumns, partitionMap);
	}
}
