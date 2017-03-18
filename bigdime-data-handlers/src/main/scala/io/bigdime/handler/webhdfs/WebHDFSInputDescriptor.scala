package io.bigdime.handler.webhdfs

import java.nio.channels.ReadableByteChannel

import io.bigdime.core.InputDescriptor
import io.bigdime.core.commons.StringHelper
import io.bigdime.libs.hdfs.FileStatus

/**
  * Created by neejain on 12/11/16.
  */
object WebHDFSInputDescriptor {
  //  private val INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.webhdfs.WebhdfsReaderAndSink,webhdfsPath:"
}

class WebHDFSInputDescriptor(inputDescriptorPrefix: String = "handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:") extends InputDescriptor[String] {
  private var fullDescriptor: String = null
  // complete filename, with prefix
  private var webhdfsPath: String = null
  // complete filename
  private var currentFileStatus: FileStatus = null
  private var currentFilePath: String = null
  // private long fileLength = -1;
  private var fileChannel: ReadableByteChannel = null


  def getNext(availableInputDescriptors: java.util.List[String], lastInputDescriptor: String): String = {
    val indexOfLastInput: Int = availableInputDescriptors.indexOf(lastInputDescriptor)
    if (availableInputDescriptors.size > indexOfLastInput + 1) {
      return availableInputDescriptors.get(indexOfLastInput + 1)
    }
    return null
  }

  def parseDescriptor(inputDescriptor: String) {
    if (StringHelper.isBlank(inputDescriptor)) {
      throw new IllegalArgumentException("descriptor can't be null or empty")
    }
    if (inputDescriptor.indexOf(inputDescriptorPrefix) < 0) {
      throw new IllegalArgumentException("descriptor must contain prefix:" + inputDescriptorPrefix)
    }
    fullDescriptor = inputDescriptor
    webhdfsPath = inputDescriptor.substring(inputDescriptorPrefix.length)
    currentFilePath = webhdfsPath
  }

  def getWebhdfsPath: String = {
    return webhdfsPath
  }

  def setWebhdfsPath(webhdfsPath: String) {
    this.webhdfsPath = webhdfsPath
  }

  def getFullDescriptor: String = {
    return fullDescriptor
  }

  def getDescriptorPrefix: String = {
    return inputDescriptorPrefix
  }

  def createFullDescriptor(webhdfsPath: String): String = {
    return inputDescriptorPrefix + webhdfsPath
  }

  def getCurrentFileStatus: FileStatus = {
    return currentFileStatus
  }

  def setCurrentFileStatus(currentFileStatus: FileStatus) {
    this.currentFileStatus = currentFileStatus
  }

  def getCurrentFilePath: String = {
    return currentFilePath
  }

  def setCurrentFilePath(currentFilePath: String) {
    this.currentFilePath = currentFilePath
  }

  def getFileLength: Long = {
    return getCurrentFileStatus.length
  }

  def getFileChannel: ReadableByteChannel = {
    return fileChannel
  }

  def setFileChannel(fileChannel: ReadableByteChannel) {
    this.fileChannel = fileChannel
  }

  def this() {
    this("handlerClass:io.bigdime.handler.webhdfs.WebHDFSReaderHandler,webhdfsPath:")
  }
}