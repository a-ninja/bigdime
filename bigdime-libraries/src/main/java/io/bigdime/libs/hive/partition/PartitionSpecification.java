/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * 
 */
package io.bigdime.libs.hive.partition;

import java.util.Map;

/**
 * @author mnamburi
 *
 */
public final class PartitionSpecification {
	public final String tableName;
	public final String databaseName;
	public final String location;
	public final Map<String, String> partitionColumns;

	public static class Builder {
		private final String tableName;
		private final String databaseName;
		private String location = null;
		private Map<String, String> partitionColumns;

		/**
		 * 
		 * @param databaseName
		 * @param tableName
		 */
		public Builder(String databaseName, String tableName) {
			this.databaseName = databaseName;
			this.tableName    = tableName;
		}
		/**
		 * 
		 * @param partitionColumns
		 * @return
		 */
		public Builder partitionColumns(Map<String, String> partitionColumns ){ 
			this.partitionColumns = partitionColumns;
			return this; 
		}
		/**
		 * 
		 * @param location
		 * @return
		 */
		public Builder location(String location ){ 
			this.location = location;
			return this; 
		}
		public PartitionSpecification build() {
			return new PartitionSpecification(this);
		}
	}
	private PartitionSpecification(Builder builder) {
		databaseName = builder.databaseName;
		tableName    = builder.tableName;
		location     = builder.location;
		partitionColumns = builder.partitionColumns;
	}
}
