/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.common;

import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatConstants;
/**
 * 
 * @author mnamburi
 *
 */
public abstract class HiveConfigManager {
	protected final HiveConf  hiveConf ;
	private static final String HIVE_DOT = "hive.";
	private static final long default_client_expiry_time = TimeUnit.MINUTES.toMillis(10);
	public HiveConfigManager(){
		this(null);
	}
	public HiveConfigManager(Properties properties){
		hiveConf = new HiveConf();
		setDefaultProperties();
		if(properties != null && properties.entrySet() != null){
			for (Entry<Object, Object> entry : properties.entrySet()) {
				String key = entry.getKey().toString();
				if (key.startsWith(HIVE_DOT)) {
					hiveConf.set(key, entry.getValue().toString());
					continue;
				}				
			}
		}
	}
	public void setDefaultProperties(){
		hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
		hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
		hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, Boolean.FALSE.toString());
		hiveConf.set(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname,Boolean.TRUE.toString());
		hiveConf.setBoolean(HCatConstants.HCAT_HIVE_CLIENT_DISABLE_CACHE, false);
		hiveConf.set(HCatConstants.HCAT_HIVE_CLIENT_EXPIRY_TIME, Long.toString(default_client_expiry_time));
	}
}
