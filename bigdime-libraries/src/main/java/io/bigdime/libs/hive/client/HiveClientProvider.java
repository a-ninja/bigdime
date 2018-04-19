/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.client;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;

public final class HiveClientProvider {
	private HiveClientProvider(){}
	/**
	 * 
	 * @param hiveConf
	 * @return
	 * @throws HCatException
	 */
	public static HCatClient getHcatClient(HiveConf hiveConf) throws HCatException{
		HCatClient client = HCatClient.create(hiveConf);
		return client;
	}
	/**
	 * 
	 * @param client
	 */
	public static void closeClient(HCatClient client){
		if(client != null){
			try {
				client.close();
			} catch (HCatException e) {
				
			}
		}
		
	}
}
