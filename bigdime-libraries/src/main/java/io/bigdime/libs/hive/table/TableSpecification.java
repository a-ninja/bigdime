/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * 
 */
package io.bigdime.libs.hive.table;

import io.bigdime.libs.hive.common.Column;

import java.util.List;

/**
 * @author mnamburi
 *
 */
public final class TableSpecification {
	public final String tableName;
	public final String databaseName;
	public final boolean isExternal;
	public final String location;
	public final List<Column> columns;
	public final List<Column> partitionColumns;
	public final String fileFormat;
	public final char fieldsTerminatedBy;
	public final char linesTerminatedBy;
	public final String owner;
	public final String comment;


	public static class Builder {
		private final String tableName;
		private final String databaseName;
		private boolean isExternal = false;
		private String location = null;
		private List<Column> partitionColumns;
		private List<Column> columns;		
		private String fileFormat;
		private char fieldsTerminatedBy;
		private char linesTerminatedBy;
		private String owner;
		private String comment;		

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
		 *  by default the table is managed table, unless user provides the location.
		 * @param location
		 * @return
		 */
		public Builder externalTableLocation(String location){
			if(location != null){
				this.location = location;
				this.isExternal = Boolean.TRUE;				
			}
			return this; 
		}
		/**
		 * 
		 * @param columns
		 * @return
		 */
		public Builder columns(List<Column> columns ){ 
			this.columns = columns;
			return this; 
		}
		/**
		 * 
		 * @param partitionColumns
		 * @return
		 */
		public Builder partitionColumns(List<Column> partitionColumns ){ 
			this.partitionColumns = partitionColumns;
			return this; 
		}
		/**
		 * 
		 * @param fileFormat
		 * @return
		 */
		public Builder fileFormat(String fileFormat){
			this.fileFormat = fileFormat;
			return this; 
		}
		/**
		 * 
		 * @param fieldsTerminatedBy
		 * @return
		 */
		public Builder fieldsTerminatedBy(char fieldsTerminatedBy){
			this.fieldsTerminatedBy = fieldsTerminatedBy;
			return this; 
		}
		/**
		 * 
		 * @param linesTerminatedBy
		 * @return
		 */
		public Builder linesTerminatedBy(char linesTerminatedBy){
			this.linesTerminatedBy = linesTerminatedBy;
			return this; 
		}
		/**
		 * 
		 * @param owner
		 * @return
		 */
		public Builder owner(String owner){
			this.owner = owner;
			return this; 
		}
		/**
		 * 
		 * @param comment
		 * @return
		 */
		public Builder comment(String comment){
			this.comment = comment;
			return this; 
		}			

		public TableSpecification build() {
			return new TableSpecification(this);
		}
	}
	private TableSpecification(Builder builder) {
		databaseName = builder.databaseName;
		tableName    = builder.tableName;
		isExternal   = builder.isExternal;
		columns 	 = builder.columns;
		location     = builder.location;
		partitionColumns = builder.partitionColumns;
		fileFormat     = builder.fileFormat;
		fieldsTerminatedBy     = builder.fieldsTerminatedBy;
		linesTerminatedBy     = builder.linesTerminatedBy;
		owner     = builder.owner;
		comment     = builder.comment;

	}
}
