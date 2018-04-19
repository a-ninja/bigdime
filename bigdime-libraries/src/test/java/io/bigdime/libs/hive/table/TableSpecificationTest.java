/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.table;

import io.bigdime.libs.hive.table.TableSpecification;
import io.bigdime.libs.hive.table.TableSpecification.Builder;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TableSpecificationTest {
	@Test
	public void testSpecficationBuild(){
		Builder tableSpecBuilder = new Builder("mockdb", "mockTable");
		TableSpecification  tableSpec = tableSpecBuilder.externalTableLocation("/tmp")
				.columns(null)
				.fieldsTerminatedBy('\001')
				.linesTerminatedBy('\001').partitionColumns(null).fileFormat("Text").build();
		Assert.assertEquals(tableSpec.location, "/tmp");
		Assert.assertEquals(tableSpec.databaseName, "mockdb");
		Assert.assertEquals(tableSpec.tableName, "mockTable");
		Assert.assertEquals(tableSpec.fieldsTerminatedBy, '\001');
		Assert.assertEquals(tableSpec.linesTerminatedBy, '\001');
		Assert.assertEquals(tableSpec.fileFormat, "Text");
	}
}
