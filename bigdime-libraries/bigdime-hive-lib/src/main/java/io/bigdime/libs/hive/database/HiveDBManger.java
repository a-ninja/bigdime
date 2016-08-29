/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.database;



import java.util.Properties;

import io.bigdime.libs.hive.client.HiveClientProvider;
import io.bigdime.libs.hive.common.HiveConfigManager;

import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.ObjectNotFoundException;
import org.apache.hive.hcatalog.api.HCatClient.DropDBMode;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc.Builder;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.common.HCatException;
import org.springframework.util.Assert;

import com.google.common.base.Preconditions;

/**
 * 
 * @author mnamburi
 *
 */
public class HiveDBManger extends HiveConfigManager {
	boolean dbCreated = false;

	public HiveDBManger(Properties properties) {
		super(properties);
	}

	public static HiveDBManger getInstance(){
		return getInstance(null);
	}

	public static HiveDBManger getInstance(Properties properties){
		return new HiveDBManger(properties);
	}

	/**
	 * 
	 * @param databaseSpecification
	 * @throws HCatException
	 */
	public synchronized void createDatabase(DatabaseSpecification databaseSpecification) throws HCatException{
		HCatClient client = null;
		HCatCreateDBDesc databaseDescriptor = null;
		String databaseName = databaseSpecification.databaseName;
		Preconditions.checkNotNull(databaseName,"databaseName cannot be null");

		String host = databaseSpecification.host;
		String location = databaseSpecification.location;
		String scheme = databaseSpecification.scheme;


		Builder builder = HCatCreateDBDesc.create(databaseName).ifNotExists(true); 
		if(databaseSpecification.location != null){
			if(scheme != null &&  host != null){
				builder.location(scheme + host + location + "/" + databaseName);
			} else {
				builder.location(location + "/" + databaseName);
			}
		}

		databaseDescriptor = builder.build();
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			client.createDatabase(databaseDescriptor);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
	/**
	 * 
	 * @param dbName
	 * @throws HCatException
	 */
	public void dropDatabase(String dbName) throws HCatException{
		HCatClient client = null;
		try {
			client = HiveClientProvider.getHcatClient(hiveConf);
			client.dropDatabase(dbName, Boolean.TRUE, DropDBMode.CASCADE);
		} catch (HCatException e) {
			throw e;
		}finally{
			HiveClientProvider.closeClient(client);
		}
	}
	/**
	 * 
	 * @param dbName
	 * @return
	 * @throws HCatException 
	 */
	public synchronized boolean isDatabaseCreated(String dbName) throws HCatException{
		HCatClient client = null;
		try {
			if(dbCreated)
				return dbCreated;
			client = HiveClientProvider.getHcatClient(hiveConf);
			HCatDatabase hcatDatabase = client.getDatabase(dbName);
			Assert.hasText(hcatDatabase.getName(), "DB is null");
			dbCreated = true;
		} catch (HCatException e) {
			if (ObjectNotFoundException.class == e.getClass()) {
				dbCreated = false;
			}
		}finally{
			HiveClientProvider.closeClient(client);
		}		
		return dbCreated;
	}
}
