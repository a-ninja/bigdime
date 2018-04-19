/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
public final class DataRetrievalSpecification {

	private String tableName;
	private Get get;
	private Scan scan;

	private DataRetrievalSpecification(Builder builder) {
		this.tableName = builder.tableName;
		this.get = builder.get;
		this.scan = builder.scan;
	}

	public String getTableName() {
		return tableName;
	}


	public Get getGet() {
		return get;
	}

	public Scan getScan() {
		return scan;
	}


	public static class Builder {
		private String tableName;
		private Get get;
		private Scan scan;

		public Builder withTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder withGet(Get get) {
			this.get = get;
			return this;
		}

		public Builder withScan(Scan scan) {
			this.scan = scan;
			return this;
		}

		public DataRetrievalSpecification build() {
			return new DataRetrievalSpecification(this);
		}

	}

}
