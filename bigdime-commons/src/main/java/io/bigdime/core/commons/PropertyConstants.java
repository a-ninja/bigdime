package io.bigdime.core.commons;

public class PropertyConstants {
	public static class CHANNEL {
		public static final String PRINT_CHANNEL_STATS = "print.channel.stats";
		public static final String PRINT_STATS_DURATION_IN_SECONDS = "print.stats.duration.in.seconds";
		public static final String DEFAULT_CHANNEL_CAPACITY = "default.channel.capacity";
	}

	public static class HBASE {
		public static final String CONNECTION_TIMEOUT = "hbase.connection.timeout";
		public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
		public static final String ZOOKEEPER_PROPERTY_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
		public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";
		public static final String POOL_SIZE = "htable.pool.size";
		public static final String TEST_USER_DATA = "hbase.test.user.data";
		public static final String TABLE_NAME = "hbase.table.name";
		public static final String ALERT_LEVEL = "hbase.alert.level";
		public static final String DEBUG_INFO_BATCH_SIZE = "hbase.debugInfo.batchSize";
	}

	public static class METADATA {
		public static final String DATABASE_DRIVERCLASSNAME = "metastore.database.driverClassName";
		public static final String SOURCENAME = "metastore.sourcename";
		public static final String HIBERNATE_MYSQL_DIALECT = "metastore.hibernate.mysql.dialect";
		public static final String HIBERNATE_HBM2DDL_AUTO = "metastore.hibernate.hbm2ddl.auto";
		public static final String HIBERNATE_SHOW_SQL = "metastore.hibernate.show_sql";
		public static final String HIBERNATE_CONNECTION_AUTOCOMMIT = "metastore.hibernate.connection.autocommit";
		public static final String DYNAMIC_DATATYPES_CONFIGURATION = "metastore.dynamicDataTypesConfiguration";
		public static final String PERSISTENCE = "metastore.persistence";
		public static final String DATABASE_URL = "metastore.database.url";
		public static final String DATABASE_USERNAME = "metastore.database.username";
		public static final String DATABASE_PASSWORD = "metastore.database.password";
	}

	public static class HDFS {
		public static final String HDFS_HOSTS = "hdfs_hosts";
		public static final String HDFS_PORT = "hdfs_port";
		/// webhdfs/v1/data/bigdime/adaptor
		public static final String HDFS_PATH = "hdfs_path";
		public static final String HDFS_USER = "hdfs_user";
	}

	public static class HIVE {
		public static final String JDBC_CONNECTION_URL = "hive.jdbc.connection.url";
		public static final String JDBC_USER_NAME = "hive.jdbc.user.name";
		/// webhdfs/v1/data/bigdime/adaptor
		public static final String BASE_OUTPUT_DIRECTORY = "hive.base.output.directory";
	}

	public static class MAPREDUCE {
		public static final String MAPREDUCE_JOB_QUEUENAME = "mapreduce.job.queuename";
	}

	public static class SWIFT {
		public static final String USER_NAME = "swift.user.name";
		public static final String PASSWORD = "swift.password";
		public static final String AUTH_URL = "swift.auth.url";
		public static final String TENANT_ID = "swift.tenant.id";
		public static final String TENANT_NAME = "swift.tenant.name";
		public static final String CONTAINER_NAME = "swift.container.name";
		public static final String ALERT_CONTAINER_NAME = "swift.alert.container.name";
		public static final String ALERT_LEVEL = "swift.alert.level";
		public static final String DEBUG_INFO_BUFFER_SIZE = "swift.debugInfo.bufferSize";
	}

}
