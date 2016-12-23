package io.bigdime.handler.swift

import java.io.{ByteArrayOutputStream, IOException}

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.{ActionEvent, HandlerException}
import io.bigdime.core.commons.{AdaptorLogger, StringHelper}
import io.bigdime.core.constants.ActionEventHeaderConstants

/**
  * Created by neejain on 12/22/16.
  */
object AbstractByteWriterHandler {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[AbstractByteWriterHandler]))
}

abstract class AbstractByteWriterHandler extends SwiftWriterHandler {

  import AbstractByteWriterHandler.logger
  import io.bigdime.util.RunContext.autoCloseable

  @throws[IOException]
  @throws[HandlerException]
  protected def writeToSwift(actionEvent: ActionEvent, actionEvents: java.util.List[ActionEvent]): ActionEvent = {
    autoCloseable(new ByteArrayOutputStream)(baos => {
      val fileName = actionEvent.getHeaders.get(ActionEventHeaderConstants.SOURCE_FILE_NAME)
      val swiftObjectName = StringHelper.replaceTokens(fileName, outputFilePathPattern, inputPattern, actionEvent.getHeaders)
      var sizeToWrite = 0
      import scala.collection.JavaConversions._
      for (thisEvent <- actionEvents) {
        sizeToWrite = sizeToWrite + thisEvent.getBody.length
        baos.write(thisEvent.getBody)
      }
      actionEvents.clear()
      val dataToWrite = baos.toByteArray
      logger.info(getHandlerPhase, "_message=\"writing to swift\" swift_object_name={} object_length={}", swiftObjectName, dataToWrite.length: java.lang.Integer)
      val object1 = swiftClient.write(swiftObjectName, dataToWrite)
      val outputEvent = new ActionEvent
      outputEvent.setHeaders(actionEvent.getHeaders)
      outputEvent.getHeaders.put(ActionEventHeaderConstants.SwiftHeaders.OBJECT_NAME, object1.getName)
      outputEvent.getHeaders.put(ActionEventHeaderConstants.SwiftHeaders.OBJECT_ETAG, object1.getEtag)
      outputEvent.setBody(dataToWrite)
      outputEvent
    })
  }
}
