package io.bigdime.alert.impl.swift

import java.util.concurrent.Callable

import org.javaswift.joss.headers.`object`.ObjectManifest
import org.javaswift.joss.instructions.UploadInstructions
import org.javaswift.joss.model.Container
import org.joda.time.format.DateTimeFormat

/**
  * Created by neejain on 2/4/17.
  */
case class SwiftLogTask(val container: Container, val sourceName: String, val message: Array[Byte]) extends Callable[Any] {
  final private[swift] val fileNameDtf = DateTimeFormat.forPattern("yyyy-MM-dd")

  @throws[Exception]
  def call: Any = {
    val timestamp = System.currentTimeMillis
    writeSegment(timestamp)
    writeManifest(timestamp)
    return null
  }

  /**
    *
    * @param timestamp
    */
  private def writeSegment(timestamp: Long) {
    val segmentName = getSegmentName(timestamp)
    val largeObject = container.getObject(segmentName)
    largeObject.uploadObject(message)
  }

  private def writeManifest(timestamp: Long) {
    val largeObjectName = getObjectName(timestamp)
    val largeObject = container.getObject(largeObjectName)
    val uploadInstructions = new UploadInstructions(Array[Byte]())
    uploadInstructions.setObjectManifest(new ObjectManifest(container.getName + "/" + getPrefixWithDash(timestamp)))
    largeObject.uploadObject(uploadInstructions)
  }

  private def getPrefixWithoutDash(timestamp: Long) = {
    val prefix = sourceName + "." + fileNameDtf.print(timestamp)
    prefix
  }

  private def getPrefixWithDash(timestamp: Long) = getPrefixWithoutDash(timestamp) + "-"

  private def getSegmentName(timestamp: Long) = getPrefixWithDash(timestamp) + timestamp

  private def getObjectName(timestamp: Long) = {
    "alerts." + sourceName + ".log" + fileNameDtf.print(timestamp)
  }
}