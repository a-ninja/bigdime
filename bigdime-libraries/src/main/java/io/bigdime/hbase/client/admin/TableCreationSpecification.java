/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client.admin;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;

/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
public final class TableCreationSpecification {
	
	private String tableName;
	private List<HColumnDescriptor> columnsFamilies;
	
	private TableCreationSpecification(Builder builder) {
		this.tableName=builder.tableName;
		this.columnsFamilies=builder.columnsFamilies;
	}
	public String getTableName() {
		return tableName;
	}
	public List<HColumnDescriptor> getColumnsFamilies() {
		return columnsFamilies;
	}

	public static class Builder{
		private String tableName;
		private List<HColumnDescriptor> columnsFamilies;

		public Builder(){
			columnsFamilies=new ArrayList<HColumnDescriptor>();
			
		}

		public Builder withTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder withColumnsFamilies(List<HColumnDescriptor> columnsFamilies) {
			this.columnsFamilies.addAll(columnsFamilies);
			return this;
		}

		public TableCreationSpecification build(){
			return new TableCreationSpecification(this);
		}
				
	}
	
}
