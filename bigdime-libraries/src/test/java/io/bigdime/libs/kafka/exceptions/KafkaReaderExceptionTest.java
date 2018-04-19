/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.kafka.exceptions;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class KafkaReaderExceptionTest extends ExceptionTester {
	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(KafkaReaderException.class);
	}

	@Test
	public void testConstructor() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithStringAndThrowable(KafkaReaderException.class);
	}
}
