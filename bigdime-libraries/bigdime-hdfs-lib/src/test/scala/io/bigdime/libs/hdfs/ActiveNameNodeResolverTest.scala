package io.bigdime.libs.hdfs

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 1/13/17.
  */
class ActiveNameNodeResolverTest {
  @Test
  def testWithSingleHostName(): Unit = {
    val actualHost = ActiveNameNodeResolver.rotateHost("http://unit-hostname", null)
    Assert.assertEquals(actualHost, "http://unit-hostname")

  }

  @Test
  def testWithMultipleHostNamesAndNullActiveHost(): Unit = {
    val actualHost = ActiveNameNodeResolver.rotateHost("http://unit-hostname-1, https://unit-hostname-2", null)
    Assert.assertEquals(actualHost, "http://unit-hostname-1")

  }

  @Test
  def testWithMultipleHostNamesAndNonNullActiveHost(): Unit = {
    val actualHost = ActiveNameNodeResolver.rotateHost("http://unit-hostname-1, https://unit-hostname-2", "http://unit-hostname-1")
    Assert.assertEquals(actualHost, "https://unit-hostname-2")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithNullHosts(): Unit = {
    val actualHost = ActiveNameNodeResolver.rotateHost(null, "not-null")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithEmptyHosts(): Unit = {
    val actualHost = ActiveNameNodeResolver.rotateHost("", "not-null")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithEmptyWhiteSpaceHosts(): Unit = {
    val actualHost = ActiveNameNodeResolver.rotateHost(" ", "not-null")
  }

}
