/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;

import org.apache.commons.lang.NotImplementedException;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * This class process the src-desc to parse input json node
 * format sql query
 * 
 * @author Pavan Sabinikari, Rita Liu
 *
 */
@Component
@Scope("prototype")
public class JdbcInputDescriptor implements InputDescriptor<String>{

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JdbcInputDescriptor.class));

	@Value("${split.size}") private String splitSize;
	
	private String query;
	private String inputType;
	private String inputValue;
	private String includeFilter;
	private String excludeFilter;
	private String entityName;
	private String databaseName;
	private String targetEntityName;
	private String targetDBName;
	private String incrementedBy;
	private String incrementedColumnType;
	private String columnName;
	private String partition;
	private String fieldDelimeter;
	private String rowDelimeter;
	private String snapshot;
	@Value("${hdfs_base}")
	private String entityLocation;
	
	private List<String> columnList = new ArrayList<String>();
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	@Override
	public String getNext(List<String> availableInputDescriptors,
			String lastInputDescriptor) throws NotImplementedException{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parseDescriptor(String jsonStr) {
		if (jsonStr == null || jsonStr.length() <= 0)
			throw new IllegalArgumentException("descriptor can't be null or empty");
		try {
			JSONObject jsonObject = new JSONObject(jsonStr);
		
		inputType = jsonObject.getString(JdbcConstants.INPUT_TYPE);
		inputValue = jsonObject.getString(JdbcConstants.INPUT_VALUE);
		incrementedBy = jsonObject.getString(JdbcConstants.INCREMENTED_BY);
		if(inputType.equalsIgnoreCase(JdbcConstants.DB_FLAG)){
			databaseName = inputValue;
			if(!jsonObject.isNull(JdbcConstants.INCLUDE_FILTER))
				includeFilter = jsonObject.getString(JdbcConstants.INCLUDE_FILTER);
			if(!jsonObject.isNull(JdbcConstants.EXCLUDE_FILTER))
				excludeFilter = jsonObject.getString(JdbcConstants.EXCLUDE_FILTER);
		}
		if (!jsonObject.isNull(JdbcConstants.PARTITIONED_COLUMNS))
			partition = jsonObject.getString(JdbcConstants.PARTITIONED_COLUMNS);
		if(partition == null || partition.length() ==0)
			partition = incrementedBy;
		if (!jsonObject.isNull(JdbcConstants.FIELD_DELIMETER))
			fieldDelimeter = jsonObject.getString(JdbcConstants.FIELD_DELIMETER);
		if (fieldDelimeter == null || fieldDelimeter.length() == 0)
			fieldDelimeter = JdbcConstants.DEFAULT_CONTROL_A_DELIMETER;
		if (!jsonObject.isNull(JdbcConstants.ROW_DELIMETER))
			rowDelimeter = jsonObject.getString(JdbcConstants.ROW_DELIMETER);
		if (rowDelimeter == null || rowDelimeter.length() == 0)
			rowDelimeter = JdbcConstants.NEW_LINE_DELIMETER;
		if(inputType.equalsIgnoreCase(JdbcConstants.TABLE_FLAG)){
			entityName = inputValue;
			if (!jsonObject.isNull(JdbcConstants.TARGET_TABLE_NAME))
				targetEntityName = jsonObject.getString(JdbcConstants.TARGET_TABLE_NAME);
			if (targetEntityName == null || targetEntityName.length() == 0)
				targetEntityName = entityName;
			databaseName = jsonObject.getString(JdbcConstants.SOURCE_DB_NAME);
		}
		if (!jsonObject.isNull(JdbcConstants.SNAPSHOT_FLAG))
			snapshot = jsonObject.getString(JdbcConstants.SNAPSHOT_FLAG);
		} catch (JSONException e) {
			logger.warn("Json Exception", "unable to parse descriptor jsonStr={}", jsonStr);
		}

		logger.debug("Retrieved Jdbc Reader Handler Configurations","inputType={} inputValue={} incrementedBy={} include={} exclude={} partitionedColumns={} fieldDelimeter={} rowDelimeter={}",
				inputType, inputValue, incrementedBy, includeFilter, excludeFilter, partition, fieldDelimeter, rowDelimeter);		
	}
	
	public String formatQuery(String inputType, String inputValue, String driverName){
		if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER)> JdbcConstants.INTEGER_CONSTANT_ZERO){
			if(!inputValue.isEmpty()){
				query = JdbcConstants.SELECT_SCHEMA_QUERY +" WHERE owner ='"+inputValue+"'";
			} else {
				query = JdbcConstants.SELECT_SCHEMA_QUERY;
			}
		}
		return query;

	}
	
	public String formatProcessTableQuery(String dbname, String tableName, String driverName) {
		if(!incrementedBy.isEmpty()){
			if(inputType.equalsIgnoreCase(JdbcConstants.DB_FLAG)){
				if(!dbname.isEmpty()){
					query = JdbcConstants.SELECT_FROM + dbname+"."+ tableName + JdbcConstants.WHERE_CLAUSE + incrementedBy + JdbcConstants.GREATER_THAN + JdbcConstants.QUERY_PARAMETER + JdbcConstants.ORDER_BY_CLAUSE + incrementedBy + JdbcConstants.ASC_ORDER;
				} else{
					query = JdbcConstants.SELECT_FROM + tableName + JdbcConstants.WHERE_CLAUSE + incrementedBy + JdbcConstants.GREATER_THAN + JdbcConstants.QUERY_PARAMETER + JdbcConstants.ORDER_BY_CLAUSE + incrementedBy + JdbcConstants.ASC_ORDER;
				}
				if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
					query = JdbcConstants.SELECT_T_FROM + query + JdbcConstants.WHERE_ROWNUM + splitSize;
				}
			}
			if(inputType.equalsIgnoreCase(JdbcConstants.TABLE_FLAG)){
				if(!dbname.isEmpty()){
					query = JdbcConstants.SELECT_FROM + dbname + "." + tableName + JdbcConstants.WHERE_CLAUSE + incrementedBy + JdbcConstants.GREATER_THAN + JdbcConstants.QUERY_PARAMETER + JdbcConstants.ORDER_BY_CLAUSE + incrementedBy + JdbcConstants.ASC_ORDER;
				} else{
					query = JdbcConstants.SELECT_FROM + tableName + JdbcConstants.WHERE_CLAUSE + incrementedBy + JdbcConstants.GREATER_THAN + JdbcConstants.QUERY_PARAMETER + JdbcConstants.ORDER_BY_CLAUSE + incrementedBy + JdbcConstants.ASC_ORDER;
				}
				if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
					query = JdbcConstants.SELECT_T_FROM + query + JdbcConstants.WHERE_ROWNUM + splitSize;
				}
			}
			if(inputType.equalsIgnoreCase(JdbcConstants.QUERY_FLAG)){
				query = inputValue;
				if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
					query = JdbcConstants.SELECT_T_FROM + query + JdbcConstants.WHERE_ROWNUM + splitSize;
				}
			}
		} 
		return query;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getInputType() {
		return inputType;
	}

	public void setInputType(String inputType) {
		this.inputType = inputType;
	}

	public String getInputValue() {
		return inputValue;
	}

	public void setInputValue(String inputValue) {
		this.inputValue = inputValue;
	}

	public String getIncludeFilter() {
		return includeFilter;
	}

	public void setIncludeFilter(String includeFilter) {
		this.includeFilter = includeFilter;
	}

	public String getExcludeFilter() {
		return excludeFilter;
	}

	public void setExcludeFilter(String excludeFilter) {
		this.excludeFilter = excludeFilter;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}
	
	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public String getTargetEntityName() {
		return targetEntityName;
	}

	public void setTargetEntityName(String targetEntityName) {
		this.targetEntityName = targetEntityName;
	}

	public String getTargetDBName() {
		return targetDBName;
	}

	public void setTargetDBName(String targetDBName) {
		this.targetDBName = targetDBName;
	}
	
	public String getIncrementedBy() {
		return incrementedBy;
	}

	public void setIncrementedBy(String incrementedBy) {
		this.incrementedBy = incrementedBy;
	}
	
	public String getIncrementedColumnType() {
		return incrementedColumnType;
	}

	public void setIncrementedColumnType(String incrementedColumnType) {
		this.incrementedColumnType = incrementedColumnType;
	}
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}


	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getFieldDelimeter() {
		return fieldDelimeter;
	}

	public void setFieldDelimeter(String fieldDelimeter) {
		this.fieldDelimeter = fieldDelimeter;
	}

	public String getRowDelimeter() {
		return rowDelimeter;
	}

	public void setRowDelimeter(String rowDelimeter) {
		this.rowDelimeter = rowDelimeter;
	}

	public String getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(String snapshot) {
		this.snapshot = snapshot;
	}

	public String getEntityLocation() {
		if(entityLocation.startsWith("/webhdfs")){
			entityLocation = entityLocation.substring(entityLocation.indexOf("v1")+2);
		}
		return entityLocation;
	}


	

}