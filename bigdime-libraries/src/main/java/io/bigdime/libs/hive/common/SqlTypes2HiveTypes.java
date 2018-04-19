/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.common;

/** JDBC SQL Types */
import io.bigdime.libs.hive.constants.SourceColumnTypeConstants;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Utility class to work with SQL types to Hive Types.
 * 
 * @author ppelluru
 *
 */
public final class SqlTypes2HiveTypes {
	private static Logger logger = LoggerFactory.getLogger(SqlTypes2HiveTypes.class);
	private SqlTypes2HiveTypes() {

	}
	
	/**
	 * stores java.sql.Types to Hive Type
	 */
	private static Map<Integer, PrimitiveTypeInfo> sqlType2HiveTypeMap = new HashMap<Integer, PrimitiveTypeInfo>();

	/**
	 * String SQL type to int java.sql.Types
	 */
	private static Map<String, Integer> string2SqlTypeMap = new HashMap<String, Integer>();
	
	public static final PrimitiveTypeInfo varcharTypeInfo = new VarcharTypeInfo(4000);

	static {
		
		// add SQL Types as key and Hive Type as Value to Map
		sqlType2HiveTypeMap.put(Types.SMALLINT, TypeInfoFactory.shortTypeInfo);
		sqlType2HiveTypeMap.put(Types.INTEGER, TypeInfoFactory.intTypeInfo);

		sqlType2HiveTypeMap.put(Types.BIGINT, TypeInfoFactory.longTypeInfo);

		sqlType2HiveTypeMap.put(Types.CLOB, TypeInfoFactory.stringTypeInfo);
		sqlType2HiveTypeMap.put(Types.CHAR, TypeInfoFactory.charTypeInfo);
		sqlType2HiveTypeMap.put(Types.LONGVARCHAR,
				TypeInfoFactory.stringTypeInfo);
		sqlType2HiveTypeMap.put(Types.NVARCHAR, TypeInfoFactory.stringTypeInfo);
		sqlType2HiveTypeMap.put(Types.NCHAR, TypeInfoFactory.stringTypeInfo);

		sqlType2HiveTypeMap.put(Types.VARCHAR, varcharTypeInfo);

		sqlType2HiveTypeMap.put(Types.DATE, TypeInfoFactory.timestampTypeInfo);

		sqlType2HiveTypeMap.put(Types.TIME, TypeInfoFactory.timestampTypeInfo);
		sqlType2HiveTypeMap.put(Types.TIMESTAMP,
				TypeInfoFactory.timestampTypeInfo);

		sqlType2HiveTypeMap.put(Types.FLOAT, TypeInfoFactory.floatTypeInfo);
		sqlType2HiveTypeMap.put(Types.REAL, TypeInfoFactory.floatTypeInfo);

		sqlType2HiveTypeMap.put(Types.DOUBLE, TypeInfoFactory.doubleTypeInfo);

		sqlType2HiveTypeMap.put(Types.NUMERIC, TypeInfoFactory.decimalTypeInfo);
		sqlType2HiveTypeMap.put(Types.DECIMAL, TypeInfoFactory.decimalTypeInfo);

		sqlType2HiveTypeMap.put(Types.BIT, TypeInfoFactory.booleanTypeInfo);
		sqlType2HiveTypeMap.put(Types.BOOLEAN, TypeInfoFactory.booleanTypeInfo);

		sqlType2HiveTypeMap.put(Types.BINARY, TypeInfoFactory.binaryTypeInfo);
		sqlType2HiveTypeMap
				.put(Types.VARBINARY, TypeInfoFactory.binaryTypeInfo);
		sqlType2HiveTypeMap.put(Types.BLOB, TypeInfoFactory.binaryTypeInfo);
		sqlType2HiveTypeMap.put(Types.LONGVARBINARY,
				TypeInfoFactory.binaryTypeInfo);
		
		//TODO this implement for java.sql.Types has STRING data type
		sqlType2HiveTypeMap.put(Types.JAVA_OBJECT, TypeInfoFactory.stringTypeInfo);
		
		// sqlType2HiveTypeMap.put(Types.OTHER,
		// TypeInfoFactory.unknownTypeInfo);

		// add String SQL Types as key and int (java.sql.Types) SQL Type Value
		// to Map

		string2SqlTypeMap.put(SourceColumnTypeConstants.SMALLINT,
				Types.SMALLINT);
		string2SqlTypeMap.put(SourceColumnTypeConstants.INT, Types.INTEGER);
		string2SqlTypeMap.put(SourceColumnTypeConstants.INTEGER, Types.INTEGER);
		string2SqlTypeMap.put(SourceColumnTypeConstants.BIGINT, Types.BIGINT);
		string2SqlTypeMap.put(SourceColumnTypeConstants.CLOB, Types.VARCHAR);
		string2SqlTypeMap.put(SourceColumnTypeConstants.VARCHAR, Types.VARCHAR);
		string2SqlTypeMap
				.put(SourceColumnTypeConstants.VARCHAR2, Types.VARCHAR);
		string2SqlTypeMap.put(SourceColumnTypeConstants.CHAR, Types.CHAR);
		string2SqlTypeMap.put(SourceColumnTypeConstants.CHAR_VARCHAR, Types.VARCHAR);
		string2SqlTypeMap.put(SourceColumnTypeConstants.LONGVARCHAR,
				Types.VARCHAR);
		string2SqlTypeMap
				.put(SourceColumnTypeConstants.NVARCHAR, Types.VARCHAR);
		string2SqlTypeMap.put(SourceColumnTypeConstants.NCHAR, Types.VARCHAR);

		string2SqlTypeMap.put(SourceColumnTypeConstants.DATE, Types.DATE);
		string2SqlTypeMap.put(SourceColumnTypeConstants.TIME, Types.TIME);
		string2SqlTypeMap.put(SourceColumnTypeConstants.TIMESTAMP,
				Types.TIMESTAMP);

		string2SqlTypeMap.put(SourceColumnTypeConstants.FLOAT, Types.FLOAT);
		string2SqlTypeMap.put(SourceColumnTypeConstants.REAL, Types.REAL);
		string2SqlTypeMap.put(SourceColumnTypeConstants.NUMBER_BIGINT, Types.BIGINT);
		string2SqlTypeMap.put(SourceColumnTypeConstants.NUMBER_DECIMAL, Types.DECIMAL);
		string2SqlTypeMap.put(SourceColumnTypeConstants.DECIMAL, Types.DECIMAL);
		string2SqlTypeMap.put(SourceColumnTypeConstants.BINARY, Types.BINARY);
		string2SqlTypeMap
				.put(SourceColumnTypeConstants.VARBINARY, Types.BINARY);
		string2SqlTypeMap.put(SourceColumnTypeConstants.BLOB, Types.BINARY);
		string2SqlTypeMap.put(SourceColumnTypeConstants.LONGVARBINARY,
				Types.BINARY);
		
		//source column type STRING mapping to string in hive
		string2SqlTypeMap.put(SourceColumnTypeConstants.STRING.toUpperCase(),
				Types.JAVA_OBJECT);

	}
	/**
	 * 
	 * @param sqlType
	 *            as String
	 * @return Hive PrimitiveTypeInfo based on java.sql.Type
	 */
	public static PrimitiveTypeInfo sqlType2HiveType(String sqlType) {

		logger.debug("sqlType :{} ************", sqlType);

		//TODO by default String
		int sqlTypeInt = Types.JAVA_OBJECT;

		if (sqlType != null) {

			if (string2SqlTypeMap.get(sqlType.toUpperCase()) != null) {
				sqlTypeInt = string2SqlTypeMap.get(sqlType.toUpperCase());
			}
		}

		logger.debug("sqlType :{} ,sqlTypeInt:{}", sqlType, sqlTypeInt);

		return sqlType2HiveType(sqlTypeInt);
	}



	/**
	 * Convert given java.sql.Types number into internal data type.
	 *
	 * @param sqlType
	 *            java.sql.Types constant
	 * @return PrimitiveTypeInfo
	 */
	public static PrimitiveTypeInfo sqlType2HiveType(int sqlType) {

		PrimitiveTypeInfo hiveType = sqlType2HiveTypeMap.get(sqlType);

		// if not found set it to String HIVE type
		if (hiveType == null) {
			hiveType = TypeInfoFactory.stringTypeInfo;
			logger.error(
					"Not able to map Database column to Hive Coloumn : {}",
					hiveType);
		}

		return hiveType;

	}



}
