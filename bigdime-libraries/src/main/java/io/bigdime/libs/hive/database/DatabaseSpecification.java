/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * 
 */
package io.bigdime.libs.hive.database;


/**
 * @author mnamburi
 *
 */
public final  class DatabaseSpecification {
	public final String host;
	public final String databaseName;
	public final String location;
	public final String scheme;
	public final String comment;
	

	public static class Builder {
		private String host;
		private final String databaseName;
		private String location;
		private String scheme;
		private String comment;
		
		/**
		 * 
		 * @param databaseName
		 */
		public Builder(String databaseName) {
			this.databaseName = databaseName;
		}
		/**
		 * 
		 * @param host
		 * @return
		 */
		public Builder host(String host){
			this.host = host;
			return this; 
		}
		/**
		 * 
		 * @param location
		 * @return
		 */
		public Builder location(String location){
			this.location = location;
			return this; 
		}
		/**
		 * 
		 * @param scheme
		 * @return
		 */
		public Builder scheme(String scheme){
			this.scheme = scheme;
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
		/**
		 * 
		 * @return
		 */
		public DatabaseSpecification build() {
			return new DatabaseSpecification(this);
		}
	}
	private DatabaseSpecification(Builder builder) {
		databaseName = builder.databaseName;
		location     = builder.location;
		comment     = builder.comment;
		scheme = builder.scheme;
		host = builder.host;
	}
}
