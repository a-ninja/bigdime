/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

public class WebHDFSSinkExceptionTest {

	@Test
	public void testConstructorWithString() {
		WebHDFSSinkException webHDFSSinkException = new WebHDFSSinkException("unit-test");
		Assert.assertTrue(webHDFSSinkException instanceof Exception);

	}

	@Test
	public void testConstructorWithStringAndThrowable() {
		WebHDFSSinkException webHDFSSinkException = new WebHDFSSinkException("unit-test", new Exception());
		Assert.assertTrue(webHDFSSinkException instanceof Exception);

	}
}
