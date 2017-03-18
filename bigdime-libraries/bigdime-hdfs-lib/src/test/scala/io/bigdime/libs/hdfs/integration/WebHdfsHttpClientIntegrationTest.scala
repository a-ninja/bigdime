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
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[List[String]](obj.getClass.getMethod("listStatus", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1")
    Assert.assertTrue(r2.head.equals("/webhdfs/v1/data/bigdime/adaptor/adaptor1user-adaptor1.txt"))
    val r3 = new WebHdfsReader("http://sandbox.hortonworks.com", 50070, null, HDFS_AUTH_OPTION.PASSWORD).list("/webhdfs/v1/data/bigdime/adaptor/adaptor1", false)
    println(r3)
  }

  @Test
  def testGetFileStatus = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[FileStatus](obj.getClass.getMethod("getFileStatus", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1/user-adaptor1.txt")
    Assert.assertNotNull(r2)
    Assert.assertTrue(r2.`type`.equals("FILE"))
  }

  @Test
  def testOpenFile = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[InputStream](obj.getClass.getMethod("open", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1/user-adaptor1.txt")
    val content = scala.io.Source.fromInputStream(r2).mkString
    println(content)
  }

  @Test
  def testGetFileChecksum = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50075, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[HdfsFileChecksum](obj.getClass.getMethod("getFileChecksum", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1/user-adaptor1.txt")
    println(s"algorithm=${r2.algorithm}, ${r2.bytes}, ${r2.length}")

    //    val content = scala.io.Source.fromInputStream(r2).mkString
    //    println(content)
  }

  @Test
  def testMkdirs = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[Boolean](obj.getClass.getMethod("mkdirs", classOf[String]), 1, "/webhdfs/v1/data/bigdime/adaptor/adaptor1/ad")
    println(s"response=$r2")
    Assert.assertNotNull(r2)
    Assert.assertEquals(r2, true)
  }

  @Test
  def testRename = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val mk = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[Boolean](obj.getClass.getMethod("mkdirs", classOf[String]), 1, "/webhdfs/v1/data/bigdime/torename")
    Assert.assertEquals(mk, true)

    val ls = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[FileStatus](obj.getClass.getMethod("getFileStatus", classOf[String]), 1, "/webhdfs/v1/data/bigdime/torename")
    Assert.assertNotNull(ls)
    Assert.assertTrue(ls.`type`.equals("DIRECTORY"))

    val r2 = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[Boolean](obj.getClass.getMethod("rename", classOf[String], classOf[String]), 1, "/webhdfs/v1/data/bigdime/torename", "/data/bigdime/renamed")
    println(s"response=$r2")
    Assert.assertNotNull(r2)
    Assert.assertEquals(r2, true)
  }

  @Test
  def testDelete = {
    val obj = new WebhdfsFacade("http://sandbox.hortonworks.com", 50070, HDFS_AUTH_OPTION.PASSWORD)
    val mk = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[Boolean](obj.getClass.getMethod("mkdirs", classOf[String]), 1, "/webhdfs/v1/data/bigdime/todel")
    Assert.assertEquals(mk, true)

    val ls = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[FileStatus](obj.getClass.getMethod("getFileStatus", classOf[String]), 1, "/webhdfs/v1/data/bigdime/todel")
    Assert.assertNotNull(ls)
    Assert.assertTrue(ls.`type`.equals("DIRECTORY"))

    val del = obj.addParameter("namenoderpcaddress", "sandbox.hortonworks.com:8020").invokeWithRetry[Boolean](obj.getClass.getMethod("delete", classOf[String]), 1, "/webhdfs/v1/data/bigdime/todel")
    Assert.assertEquals(del, true)
  }


}
