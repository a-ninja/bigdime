/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

public class JdbcConstants {

	public static String DEFAULT_CONTROL_A_DELIMETER = "\u0001";
	public static String NEW_LINE_DELIMETER = "\n";
	public static String QUERY_PARAMETER = "?";
	public static String SELECT_FROM = "SELECT * FROM ";
	public static String ORACLE_DRIVER = "OracleDriver";
	public static String ORACLE_DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";
	public static String ORDER_BY_CLAUSE = " ORDER BY ";
	public static String METADATA_SCHEMA_TYPE="HIVE";
	public static String HDFS_DATE_FORMAT="yyyyMMdd";
	public static String FIELD_CHARACTERS_TO_REPLACE="\n|\r";
	public static String FIELD_CHARACTERS_REPLACE_BY = "";
	public static int INTEGER_CONSTANT_NONZERO = 1;
	public static int INTEGER_CONSTANT_ZERO = 0;
	public static String HIVE_DB_NAME="hiveDBName";
	public static String SOURCE_DB_NAME = "databaseName";
	public static String WITH_NO_PARTITION = "withNoPartition";
	public static String INPUT_TYPE = "inputType";
	public static String INPUT_VALUE = "inputValue";
	public static String INCREMENTED_BY = "incrementedBy";
	public static String INCLUDE_FILTER = "include";
	public static String EXCLUDE_FILTER = "exclude";
	public static String PARTITIONED_COLUMNS = "partitionedColumns";
	public static String FIELD_DELIMETER = "fieldDelimeter";
	public static String ROW_DELIMETER = "rowDelimeter";
	public static String TARGET_TABLE_NAME = "targetTableName";
	public static String SNAPSHOT_FLAG = "snapshot";
	public static String DB_FLAG = "database";
	public static String QUERY_FLAG = "sqlQuery";
	public static String TABLE_FLAG = "table";
	public static String SELECT_SCHEMA_QUERY = "SELECT table_name FROM all_tables";
	public static String WHERE_CLAUSE = " WHERE ";
	public static String GREATER_THAN = " > ";
	public static String ASC_ORDER = " ASC";
	public static String SELECT_T_FROM = "SELECT t.* FROM ( ";
	public static String WHERE_ROWNUM = " ) t WHERE ROWNUM <";
}