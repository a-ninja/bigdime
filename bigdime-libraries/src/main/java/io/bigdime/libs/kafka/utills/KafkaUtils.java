/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.utills;

import java.util.Arrays;
import java.util.List;

/**
 * 
 * @author mnamburi Utils class to support Kafka Helper functions.
 */
public class KafkaUtils {
	private static final String DEFAULT_APPENDER = ",";

	/**
	 * parserBroker : helper function to parse the broker details. Input :
	 * {"brokers": "kafka.provider.one:9092,kafka.provider.two:9096",","} OutPut
	 * : [kafka.provider.one:9092,kafka.provider.two:9096]
	 * 
	 * @param brokers
	 * @param appender
	 * @return
	 */
	public static List<String> parseBrokers(final String brokers, final String delimiter) {
		String tempDelimiter = delimiter;
		if (tempDelimiter == null) {
			tempDelimiter = DEFAULT_APPENDER;
		}
		String[] strings = brokers.toString().split(tempDelimiter);
		return Arrays.asList(strings);
	}

}
