/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import io.bigdime.common.testutils.GetterSetterTestHelper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.reflect.FieldUtils;

import static org.mockito.MockitoAnnotations.initMocks;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JdbcInputDescriptorTest {
	

	JdbcInputDescriptor jdbcInputDescriptor;
	
	@BeforeClass
	public void init() {
		jdbcInputDescriptor = new JdbcInputDescriptor();
		initMocks(this);
	}
	
	@Test
	public void testQueryFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "query", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testInputTypeGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "inputType", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testInputValueGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "inputValue", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testIncludeGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "includeFilter", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testExcludeGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "excludeFilter", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testEntityNameFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "entityName", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testDBNameGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "databaseName", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testTargetEntityGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "targetEntityName", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testTargetDBGettersAndSetters() {
		jdbcInputDescriptor.setTargetDBName("testTargetDB");
		Assert.assertEquals(jdbcInputDescriptor.getTargetDBName(), "testTargetDB");
	}
	
	@Test
	public void testIncrementedByFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "incrementedBy", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testIncrementedColumnTypeFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "incrementedColumnType", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	
	@Test
	public void testColumnNameFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "columnName", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
		
	}
	
	@Test
	public void testPartitionFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "partition", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testFieldDelimeterFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "fieldDelimeter", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testRowDelimeterFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "rowDelimeter", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	
	@Test
	public void testSnapshotFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "snapshot", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testEntityLocationFieldGettersAndSetters() {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "entityLocation", "/webhdfs/v1/testEntityLocationPath");
		Assert.assertEquals(jdbcInputDescriptor.getEntityLocation(), "/testEntityLocationPath");
	}
	
	@Test
	public void testColumnListGettersAndSetters() {
		List<String> columnList = new ArrayList<String>();
		columnList.add("col1");
		jdbcInputDescriptor.setColumnList(columnList);
		for(String str : jdbcInputDescriptor.getColumnList()){
			Assert.assertEquals(str, "col1");
		}
	}
	
	@Test
	public void testFormatQueryWithoutDBName() {
		String query = jdbcInputDescriptor.formatQuery("database", "", "testOracleDriver");
		Assert.assertNotNull(query);
	}
	
	@Test
	public void testFormatQueryWithDBName() {
		String query = jdbcInputDescriptor.formatQuery("database", "test", "testOracleDriver");
		Assert.assertNotNull(query);
	}
	
	@Test
	public void testProcessedQueryWithEmptyDB() {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "inputType", "database");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "12");
		String query = jdbcInputDescriptor.formatProcessTableQuery("", "test", "testOracleDriver");
		Assert.assertNotNull(query);
	}
	
	@Test
	public void testProcessedQueryWithDB() {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "inputType", "database");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "12");
		String query = jdbcInputDescriptor.formatProcessTableQuery("db", "test", "testOracleDriver");
		Assert.assertNotNull(query);
	}
	
	@Test
	public void testProcessedQueryWithIncrColumnAndEmptyDB() {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "inputType", "table");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "12");
		String query = jdbcInputDescriptor.formatProcessTableQuery("", "test", "testOracleDriver");
		Assert.assertNotNull(query);
	}
	
	@Test
	public void testProcessedQueryWithIncrColumnAndDB() {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "inputType", "table");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "12");
		String query = jdbcInputDescriptor.formatProcessTableQuery("db", "test", "testOracleDriver");
		Assert.assertNotNull(query);
	}

	@Test
	public void testProcessedQuerySQLQuery() {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "inputType", "sqlQuery");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "inputValue", "testQuery");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "12");
		String query = jdbcInputDescriptor.formatProcessTableQuery("db", "test", "testOracleDriver");
		Assert.assertNotNull(query);
	}
	
	@Test
	public void testParseDescriptorWithDB(){
		jdbcInputDescriptor.parseDescriptor("{\"inputType\":\"database\",\"inputValue\":\"testDB\", \"incrementedBy\":\"testIncrementedByColumn\", \"include\":\"^regex\", \"exclude\":\"^regex\"}");
		Assert.assertNotNull(jdbcInputDescriptor.getInputType());
		Assert.assertNotNull(jdbcInputDescriptor.getInputValue());
		Assert.assertNotNull(jdbcInputDescriptor.getIncrementedBy());
		Assert.assertNotNull(jdbcInputDescriptor.getDatabaseName());
		Assert.assertNotNull(jdbcInputDescriptor.getIncludeFilter());
		Assert.assertNotNull(jdbcInputDescriptor.getExcludeFilter());
		Assert.assertNotNull(jdbcInputDescriptor.getFieldDelimeter());
		Assert.assertNotNull(jdbcInputDescriptor.getRowDelimeter());
	}
	
	@Test
	public void testParseDescriptorWithTable(){
		jdbcInputDescriptor.parseDescriptor("{\"inputType\":\"table\",\"inputValue\":\"testTable\", \"databaseName\":\"testDB\", \"incrementedBy\":\"testIncrementedByColumn\", \"targetTableName\":\"hiveTable\", \"partitionedColumns\":\"partitionedCol\", \"rowDelimeter\":\"|\", \"fieldDelimeter\":\",\"}");
		Assert.assertNotNull(jdbcInputDescriptor.getInputType());
		Assert.assertNotNull(jdbcInputDescriptor.getInputValue());
		Assert.assertNotNull(jdbcInputDescriptor.getIncrementedBy());
		Assert.assertNotNull(jdbcInputDescriptor.getDatabaseName());
		Assert.assertNotNull(jdbcInputDescriptor.getIncludeFilter());
		Assert.assertNotNull(jdbcInputDescriptor.getPartition());
		Assert.assertNotNull(jdbcInputDescriptor.getFieldDelimeter());
		Assert.assertNotNull(jdbcInputDescriptor.getRowDelimeter());
		Assert.assertNotNull(jdbcInputDescriptor.getTargetEntityName());
	}
	
	@Test
	public void testParseDescriptor(){
		jdbcInputDescriptor.parseDescriptor("{\"inputType\":\"table\",\"inputValue\":\"test\", \"incrementedBy\":\"testIncrementedByColumn\", \"targetTableName\":\"\", \"rowDelimeter\":\"\", \"fieldDelimeter\":\"\"}");
		Assert.assertNotNull(jdbcInputDescriptor.getInputType());
		Assert.assertNotNull(jdbcInputDescriptor.getInputValue());
		Assert.assertNotNull(jdbcInputDescriptor.getIncrementedBy());
		Assert.assertNotNull(jdbcInputDescriptor.getEntityName());
		Assert.assertNotNull(jdbcInputDescriptor.getFieldDelimeter());
		Assert.assertNotNull(jdbcInputDescriptor.getRowDelimeter());
		Assert.assertNotNull(jdbcInputDescriptor.getTargetEntityName());
	}
	
	@Test
	public void testParseDescriptorWithSnapshot() {
		jdbcInputDescriptor.parseDescriptor("{\"inputType\":\"database\",\"inputValue\":\"testDB\", \"incrementedBy\":\"testIncrementedByColumn\", \"include\":\"^regex\", \"snapshot\":\"SNAPSHOT\"}");
		Assert.assertNotNull(jdbcInputDescriptor.getSnapshot());
	}
	
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testParseDescriptorException() {
		jdbcInputDescriptor.parseDescriptor(null);
		
	}
	
	@Test
	public void testUnableToParseJsonException(){
		jdbcInputDescriptor.parseDescriptor("testJson");
	}
	
	@Test
	public void testGetNext() throws NotImplementedException{
		jdbcInputDescriptor.getNext(null, null);
	}

}