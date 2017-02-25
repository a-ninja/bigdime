package io.bigdime.alert.impl.swift

import java.util.concurrent.Callable

import io.bigdime.libs.client.SwiftAlertClient
import io.bigdime.util.RetryUntilSuccessful
import org.javaswift.joss.client.factory.AccountFactory
import org.javaswift.joss.model.Container
import org.joda.time.format.DateTimeFormat

/**
  * Created by neejain on 2/4/17.
  */
case class SwiftLogTask(val accountFactory: AccountFactory, containerName: String, val sourceName: String, val message: Array[Byte]) extends Callable[Any] {
  final private[swift] val fileNameDtf = DateTimeFormat.forPattern("yyyy-MM-dd")
  var container: Container = _

  @throws[Exception]
  def call: Any = {
    val timestamp = System.currentTimeMillis
    val ts = List[Class[_ <: Throwable]](classOf[Throwable])
    RetryUntilSuccessful(ts)(() => {
      container = accountFactory.createAccount.getContainer(containerName)
      val client = new SwiftAlertClient(container)
      client.writeSegment(getSegmentName(timestamp), message)
      client.writeManifest(getObjectName(timestamp), container.getName + "/" + getPrefixWithDash(timestamp))
    })
  }

  private def getPrefixWithoutDash(timestamp: Long) = {
    sourceName + "." + fileNameDtf.print(timestamp)
  }

  private def getPrefixWithDash(timestamp: Long) = getPrefixWithoutDash(timestamp) + "-"

  private def getSegmentName(timestamp: Long) = getPrefixWithDash(timestamp) + timestamp

  private def getObjectName(timestamp: Long) = {
    "alerts." + sourceName + ".log" + fileNameDtf.print(timestamp)
  }
}