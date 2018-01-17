/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.hbase.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

/**
 * 
 * @author Sandeep Reddy,Murthy,mnamburi
 * 
 */
public final class ConnectionFactory {
	// private static ConnectionFactory instance = null;
	// private HConnection globalConnection = null;

	// /**
	// * mnamburi
	// * @param hbaseConfig
	// * @throws HBaseClientException
	// */
	// private ConnectionFactory(Configuration hbaseConfig) throws
	// HBaseClientException {
	// try {
	// globalConnection = HConnectionManager.createConnection(hbaseConfig);
	// } catch (Exception e) {
	// throw new HBaseClientException("Unable to create connection to HBase",
	// e);
	// }
	// }
	//
	// public static HConnection getInstance(Configuration hbaseConfig) throws
	// HBaseClientException {
	// if ( hbaseConfig == null ) {
	// return null;
	// }
	// if (instance == null) {
	// synchronized (ConnectionFactory.class) {
	// if (instance == null) {
	// instance = new ConnectionFactory(hbaseConfig);
	// }
	// }
	// }
	// return instance.globalConnection;
	// }
	//
	// public static HConnection getInstance() {
	// return instance.globalConnection;
	// }

	//
	// public synchronized static void close() throws HBaseClientException{
	// if (instance != null) {
	// try {
	// instance.globalConnection.close();
	// instance = null;
	// } catch (Exception e){
	// throw new HBaseClientException("Unable to close HConnection", e);
	// }
	// }
	// }

	public static HConnection getInstanceofHConnection(
			Configuration configuration) throws IOException {
		HConnection hConnection = null;
		hConnection = HConnectionManager.createConnection(configuration);
		return hConnection;
	}

	// public static void close(Configuration configuration){
	// HConnectionManager.deleteConnection(configuration);
	// }

	public static void close(HConnection hConnection) throws IOException {
		hConnection.close();
	}

}
