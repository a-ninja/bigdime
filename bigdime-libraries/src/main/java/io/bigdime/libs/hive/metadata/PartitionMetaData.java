/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.metadata;

import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.common.ColumnMetaDataUtil;

import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

/**
 * @author jbrinnand
 */
public class PartitionMetaData {
	private HCatPartition partition;
	
	public PartitionMetaData(HCatPartition partition) {
		this.partition = partition;
	}
	public String getDatabaseName() {
		return partition.getDatabaseName();
	}	
	public String getTableName() {
		return partition.getTableName();
	}
	public String getLocation() {
		return partition.getLocation();
	}
	public Map<String, String> getParameters() {
		return partition.getParameters();
	}	
	public List<Column> getColumns() {
		List<HCatFieldSchema> hcatColumns = partition.getColumns();
		List<Column> columns = ColumnMetaDataUtil.addColumns(hcatColumns); 
		return columns;
	}	
	public List<String> getValues() {
		return partition.getValues();
	}	
}