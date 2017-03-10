package io.bigdime.libs.hdfs

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 3/7/17.
  */
@Test(singleThreaded = true)
class WebHdfsFactoryTest {

  @Test
  def testGetWebHdfsWithKerberos = {
    val webhdfs = WebHdfsFactory.getWebHdfs("unit-host", 1, "unit-user", HDFS_AUTH_OPTION.KERBEROS)
    Assert.assertSame(webhdfs.getClass, classOf[WebHdfsWithKerberosAuth], "WebHdfs object should be an instance of WebHdfsWithKerberosAuth")
  }

  @Test
  def testGetWebHdfsWithPassword = {
    val webhdfs = WebHdfsFactory.getWebHdfs("unit-host", 1, null, HDFS_AUTH_OPTION.PASSWORD)
    Assert.assertSame(webhdfs.getClass, classOf[WebHdfs], "WebHdfs object should be an instance of WebHdfs")
    print(webhdfs.buildURI("uni-op", "unit-path").uri.toString)
  }

  @Test
  def testGetWebHdfsWithPasswordAndUser = {
    val webhdfs = WebHdfsFactory.getWebHdfs("unit-host", 1, "unit-user", HDFS_AUTH_OPTION.PASSWORD)
    Assert.assertSame(webhdfs.getClass, classOf[WebHdfs], "WebHdfs object should be an instance of WebHdfs")
    assertUriContainsParam(webhdfs.buildURI("uni-op", "unit-path").uri.toString, "user.name")
  }

  def assertUriContainsParam(uri: String, name: String) = {
    Assert.assertTrue(uri.matches(".*[\\&\\?]" + name + "=.*"), "uri=" + uri + " should have " + name + " param in it")
  }

  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testGetWebHdfsWithNullOption = {
    WebHdfsFactory.getWebHdfs("unit-host", 1, "unit-user", null)
  }
}
