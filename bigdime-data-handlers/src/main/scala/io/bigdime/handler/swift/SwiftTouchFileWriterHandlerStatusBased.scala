package io.bigdime.handler.swift

import java.util

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.ActionEvent.Status
import io.bigdime.core.commons.{AdaptorLogger, PropertyHelper, StringHelper}
import io.bigdime.core.constants.ActionEventHeaderConstants
import io.bigdime.core.runtimeinfo.{RuntimeInfo, RuntimeInfoStore}
import io.bigdime.core.{ActionEvent, AdaptorConfigurationException, HandlerException}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
  * Construct the inputDescriptor prefix using input-descriptor-prefix-pattern, and the sourceFileName and fetch all the runtime info records.
  * If all the records have been VALIDATED, write the touch file, else, BACKOFF.
  * TODO: Cache the VALIDATED records, based on the parentRuntimeId to avoid fetching the information from DB evertytime.
  * Created by neejain on 12/8/16.
  */
object SwiftTouchFileWriterHandlerStatusBased {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(getClass))
}

@Component
@Scope("prototype")
class SwiftTouchFileWriterHandlerStatusBased extends AbstractByteWriterHandler {

  import SwiftTouchFileWriterHandlerStatusBased.logger

  protected var fileNameFromDescriptorPattern: String = _
  protected var inputDescriptorPrefixPattern: String = _
  @Autowired private val runtimeInfoStore: RuntimeInfoStore[RuntimeInfo] = null


  @throws[AdaptorConfigurationException]
  override def build() {
    // If the objectName = 20160101__part1_item_part3_part4/000304_0.ext,
    // prefix is 20160101__part1_item_part3_part4
    super.build()
    setHandlerPhase("building SwiftTouchFileWriterHandler")
    fileNameFromDescriptorPattern = PropertyHelper.getStringProperty(getPropertyMap, SwiftWriterHandlerConstants.FILE_NAME_FROM_DESCRIPTOR_PATTERN)
    inputDescriptorPrefixPattern = PropertyHelper.getStringProperty(getPropertyMap, SwiftWriterHandlerConstants.INPUT_DESCRIPTOR_PREFIX_PATTERN)
    logger.info(getHandlerPhase, "inputFilePathPattern={} fileNameFromDescriptorPattern={} inputDescriptorPrefixPattern={}", inputFilePathPattern, fileNameFromDescriptorPattern, inputDescriptorPrefixPattern)
  }

  @throws[HandlerException]
  override def process: ActionEvent.Status = {
    setHandlerPhase("processing SwiftTouchFileWriterHandlerStatusBased")
    super.process
  }

  private def parseOneToken(text: String, pattern: String) = {
    val exp = new scala.util.matching.Regex(pattern, "token")
    for (m <- exp findFirstMatchIn text) yield m group "token"
  }

  private def getJournal(): SwiftWriterHandlerJournal = {
    Option(getJournal(classOf[SwiftWriterHandlerJournal])).getOrElse({
      getHandlerContext.setJournal(getId, new SwiftWriterHandlerJournal)
      getJournal(classOf[SwiftWriterHandlerJournal])
    })
  }

  override protected def process0(actionEvents: util.List[ActionEvent]): Status = {
    val journal = getJournal
    var statusToReturn = Status.READY
    val startTime = System.currentTimeMillis
    try {
      val actionEvent = actionEvents.get(0)

      logger.info(getHandlerPhase, "actionEvents.size={}", actionEvents.size.toString)

      logger.info(getHandlerPhase, "actionEvent.headers={}", actionEvent.getHeaders)

      // /webhdfs/v1/user/johndow/bigdime/newdir3/20160101/part1_prt2_part3/0001_0.ext

      val entityName = actionEvent.getHeaders.get(ActionEventHeaderConstants.ENTITY_NAME)
      val sourceFileName = actionEvent.getHeaders.get(ActionEventHeaderConstants.SOURCE_FILE_NAME)

      logger.info(getHandlerPhase, "sourceFileName={}", sourceFileName)
      val inputDescriptorPrefix = StringHelper.replaceTokens(sourceFileName, inputDescriptorPrefixPattern, inputPattern, actionEvent.getHeaders)
      logger.debug(getHandlerPhase, "inputDescriptorPrefix={}", inputDescriptorPrefix)
      val runtimeInfos = getAllRuntimeInfos(runtimeInfoStore, entityName, inputDescriptorPrefix)

      if (runtimeInfos != null)
        logger.info(getHandlerPhase, "runtimeInfos.size={}", runtimeInfos.size.toString)

      statusToReturn = (runtimeInfos == null || runtimeInfos.isEmpty) match {
        case true => Status.BACKOFF_NOW

        case _ =>
          logger.debug(getHandlerPhase, "runtimeInfos.size={}", runtimeInfos.size.toString)
          val fileNames = for (rti <- runtimeInfos; if (rti.getStatus == RuntimeInfoStore.Status.VALIDATED)) yield parseOneToken(rti.getInputDescriptor, fileNameFromDescriptorPattern).get
          logger.debug(getHandlerPhase, "fileNames.size={} runtimeInfos.size={}", fileNames.size.toString, runtimeInfos.size().toString)

          (fileNames.size == runtimeInfos.size()) match {
            case true =>
              val numEventsToWrite: Int = 1
              logger.debug(getHandlerPhase, "_message=\"calling writeToSwift\" numEventsToWrite={}", numEventsToWrite.toString)

              actionEvent.setBody(fileNames.mkString("\n").getBytes)
              val eventListToWrite = actionEvents.subList(0, numEventsToWrite)
              val outputEvent = writeToSwift(actionEvent, eventListToWrite)
              getHandlerContext.createSingleItemEventList(outputEvent)
              journal.reset()
              eventListToWrite.clear()
              if (!actionEvents.isEmpty) {
                logger.debug(getHandlerPhase, "_message=\"returning callback\" actionEvents.size={}", actionEvents.size.toString)
                journal.setEventList(actionEvents)
                Status.CALLBACK
              }
              else {
                logger.debug(getHandlerPhase, "_message=\"returning ready\" actionEvents.size={}", actionEvents.size.toString)
                Status.READY
              }
            case _ => Status.BACKOFF_NOW // no need to call next
            // handler
          }
      }
    } catch {
      case e: Exception => throw new HandlerException(e.getMessage(), e)
    }
    val endTime = System.currentTimeMillis();
    logger.debug(getHandlerPhase(), "statusToReturn={}", statusToReturn);
    logger.info(getHandlerPhase(), "SwiftTouchFileWriterHandlerStatusBased finished in {} milliseconds",
      (endTime - startTime).toString);
    statusToReturn
  }

}
