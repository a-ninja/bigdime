/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client.admin;
/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
public final class TableDeletionSpecification {
	private String tableName;

	private TableDeletionSpecification(Builder builder) {
		this.tableName = builder.tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public static class Builder {
		private String tableName;

        public Builder withTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public TableDeletionSpecification build() {
			return new TableDeletionSpecification(this);
		}
	}
}
