package io.bigdime.libs.hdfs

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 1/13/17.
  */

@Test(singleThreaded = true)
class ActiveNameNodeResolverTest {
  val hostNames = "http://unit-hostname-1, https://unit-hostname-2"

  /**
    * Upon first invocation, rotateHost method should always return the first name from the list.
    * Subsequent invocations should rotate the host names.
    */
  @Test
  def testWithMultipleHostNamesAndNonNullActiveHost(): Unit = {

    var actualHost = ActiveNameNodeResolver.rotateHost(hostNames, "http://unit-hostname-1")
    Assert.assertEquals(actualHost, "http://unit-hostname-1")

    actualHost = ActiveNameNodeResolver.rotateHost(hostNames, "http://unit-hostname-1")
    Assert.assertEquals(actualHost, "https://unit-hostname-2")

    actualHost = ActiveNameNodeResolver.rotateHost(hostNames, "https://unit-hostname-2")
    Assert.assertEquals(actualHost, "http://unit-hostname-1")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithNullHosts(): Unit = {
    ActiveNameNodeResolver.rotateHost(null, "not-null")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithEmptyHosts(): Unit = {
    ActiveNameNodeResolver.rotateHost("", "not-null")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testNegativeWithEmptyWhiteSpaceHosts(): Unit = {
    ActiveNameNodeResolver.rotateHost(" ", "not-null")
  }

}
