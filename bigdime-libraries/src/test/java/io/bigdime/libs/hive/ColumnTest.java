/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.libs.hive.common.Column;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ColumnTest {
	@Test
	public void testGettersAndSetters() {
		Column column = new Column();
		GetterSetterTestHelper.doTest(column, "name", "unit-test-stringField");
		GetterSetterTestHelper.doTest(column, "type", "STRING");
		GetterSetterTestHelper.doTest(column, "comment", "unit-test-stringField");
		column = new Column("testColumn","String","testComment");
		Assert.assertEquals("testColumn", column.getName());
		Assert.assertEquals("String", column.getType());
		Assert.assertEquals("testComment", column.getComment());
	}
}
