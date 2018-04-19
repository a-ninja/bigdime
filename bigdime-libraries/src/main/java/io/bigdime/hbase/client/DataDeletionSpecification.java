/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.client;

import org.apache.hadoop.hbase.client.Delete;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Sandeep Reddy,Murthy ,mnamburi
 * 
 */
@Component
@Scope("prototype")
public final class DataDeletionSpecification {
	private String tableName;
	private List<Delete> deletes;

	private DataDeletionSpecification(Builder builder) {
		this.tableName = builder.tableName;
		this.deletes = builder.deletes;
	}

	public String getTableName() {
		return tableName;
	}

	public List<Delete> getDeletes() {
		return deletes;
	}

	public static class Builder {
		private String tableName;
		private List<Delete> deletes;

		public Builder() {
			deletes = new ArrayList<Delete>();
		}

		public Builder withTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder withDelete(Delete delete) {
			deletes.add(delete);
			return this;
		}

		public Builder withDeletes(List<Delete> deletes) {
			this.deletes.addAll(deletes);
			return this;
		}

		public DataDeletionSpecification build() {
			return new DataDeletionSpecification(this);
		}

	}

}
