package io.bigdime.commons

import io.bigdime.core.commons.HostNameUtils
import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 1/13/17.
  */
class HostNameUtilsTest {
  @Test
  def testWithSingleHostName(): Unit = {
    val actualHost = HostNameUtils.rotateHost("http://unit-hostname", null)
    Assert.assertEquals(actualHost, "http://unit-hostname")

  }

  @Test
  def testWithMultipleHostNamesAndNullActiveHost(): Unit = {
    val actualHost = HostNameUtils.rotateHost("http://unit-hostname-1, https://unit-hostname-2", null)
    Assert.assertEquals(actualHost, "http://unit-hostname-1")

  }

  @Test
  def testWithMultipleHostNamesAndNonNullActiveHost(): Unit = {
    val actualHost = HostNameUtils.rotateHost("http://unit-hostname-1, https://unit-hostname-2", "http://unit-hostname-1")
    Assert.assertEquals(actualHost, "https://unit-hostname-2")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithNullHosts(): Unit = {
    val actualHost = HostNameUtils.rotateHost(null, "not-null")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithEmptyHosts(): Unit = {
    val actualHost = HostNameUtils.rotateHost("", "not-null")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithEmptyWhiteSpaceHosts(): Unit = {
    val actualHost = HostNameUtils.rotateHost(" ", "not-null")
  }

}
