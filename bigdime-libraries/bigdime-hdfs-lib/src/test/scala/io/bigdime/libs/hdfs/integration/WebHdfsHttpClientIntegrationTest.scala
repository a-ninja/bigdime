package io.bigdime.libs.hdfs.integration

import java.io.InputStream

import io.bigdime.libs.hdfs._
import org.testng.Assert
import org.testng.annotations.Test

//import scala.sys.process.processInternal.InputStream

/**
  * Created by neejain on 3/12/17.
  */
@Test(singleThreaded = true)
class WebHdfsHttpClientIntegrationTest {
  @Test
  def testListStatus = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[java.util.List[String]](obj.getClass.getMethod("listStatus", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1")
    Assert.assertTrue(r2.get(0).equals("/webhdfs/v1/data/bigdime/adaptor/adaptor1user-adaptor1.txt"))
    val r3 = new WebHdfsReader("http://sandbox.hortonworks.com", 50070, null, HDFS_AUTH_OPTION.PASSWORD).list("/webhdfs/v1/data/bigdime/adaptor/adaptor1", false)
    println(r3)
  }

  @Test
  def testGetFileStatus = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[FileStatus](obj.getClass.getMethod("getFileStatus", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1/user-adaptor1.txt")
    Assert.assertNotNull(r2)
    Assert.assertTrue(r2.getType.equals("FILE"))
  }

  @Test
  def testOpenFile = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[InputStream](obj.getClass.getMethod("open", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1/user-adaptor1.txt")
    val content = scala.io.Source.fromInputStream(r2).mkString
    println(content)
  }
}
