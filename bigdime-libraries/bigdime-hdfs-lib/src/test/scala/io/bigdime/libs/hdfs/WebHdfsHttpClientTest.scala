package io.bigdime.libs.hdfs

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 3/12/17.
  */
@Test(singleThreaded = true)
class WebHdfsHttpClientTest {
  @Test
  def testWebHdfsHttpClientKerberosConstructor = {
    val obj = WebHdfsHttpClientWithKerberos
    WebHdfsHttpClientWithKerberos
    Assert.assertEquals(obj.getClass, io.bigdime.libs.hdfs.WebHdfsHttpClientWithKerberos.getClass)
  }

  @Test
  def testWebHdfsHttpClientKerberosDefaultConstructor = {
    val obj = WebHdfsHttpClient
    Assert.assertEquals(obj.getClass, io.bigdime.libs.hdfs.WebHdfsHttpClient.getClass)
  }
}
