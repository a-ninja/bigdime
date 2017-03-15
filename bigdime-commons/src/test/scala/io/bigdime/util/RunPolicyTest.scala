package io.bigdime.util

import java.io.IOException
import java.lang.reflect.InvocationTargetException

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 2/5/17.
  */
class RunPolicyTest {
  @Test(expectedExceptions = Array(classOf[RuntimeException]))
  def testRetryWithToThrow() = {
    val ts = List[Class[_ <: Throwable]](classOf[java.sql.SQLException], classOf[IOException], classOf[InvocationTargetException])
    Retry(2, ts, delay = 1, toThrow = classOf[RuntimeException])(() => {
      throw new IOException("IOException")
    })
    Assert.fail("should have thrown RuntimeException")
  }

  @Test(expectedExceptions = Array(classOf[RetriesExhaustedException]))
  def testRetryWithAttemptsAndRetriablesWithException(): Unit = {
    val ts = List[Class[_ <: Throwable]](classOf[java.sql.SQLException], classOf[IOException], classOf[InvocationTargetException])
    Retry(2, ts)(() => {
      throw new IOException("IOException")
    })
    Assert.fail("should have thrown RetriesExhaustedException")
  }

  @Test
  def testRetryWithAttemptsAndRetriables(): Unit = {
    val ts = List[Class[_ <: Throwable]](classOf[java.sql.SQLException], classOf[IOException], classOf[InvocationTargetException])
    var c = 0
    val resp = Retry(5, ts)(() => {
      if (c == 0) {
        c = 1
        throw new IOException("IOException")
      }
      "testRetryWithAttemptsAndRetriables ran successfully"
    })
    Assert.assertEquals(resp.get, "testRetryWithAttemptsAndRetriables ran successfully")
  }

  @Test(expectedExceptions = Array(classOf[IOException]))
  def testRetryWithUnretriableException(): Unit = {
    val ts = List[Class[_ <: Throwable]](classOf[java.sql.SQLException], classOf[InvocationTargetException])
    var c = 0
    Retry(5, ts)(() => {
      if (c == 0) {
        c = 1
        throw new IOException("IOException")
      }
      "testRetryWithAttemptsAndRetriables ran successfully"
    })
    Assert.fail("should have thrown IOException")
  }
}
