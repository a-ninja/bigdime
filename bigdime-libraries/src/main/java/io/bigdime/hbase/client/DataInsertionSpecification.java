/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
public final class DataInsertionSpecification {

	private String tableName;
	private List<Put> puts;

	private DataInsertionSpecification(Builder builder) {
		this.tableName = builder.tableName;
		this.puts = builder.puts;
	}

	public String getTableName() {
		return tableName;
	}

	public List<Put> getPuts() {
		return puts;
	}


	public static class Builder {
		private String tableName;
		private List<Put> puts;
		
		public Builder(){
			puts = new ArrayList<Put>();
		}

		public Builder withTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}
		
		public Builder withtPut(Put put) {
			this.puts.add(put);
			return this;
		}
    
		public Builder withtPuts(List<Put> puts) {
			this.puts.addAll(puts);
			return this;
		}

		public DataInsertionSpecification build() {
			return new DataInsertionSpecification(this);
		}

	}
}
