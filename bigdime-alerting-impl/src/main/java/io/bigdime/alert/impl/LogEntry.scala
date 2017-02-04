package io.bigdime.alert.impl

import java.util.UUID

import io.bigdime.util.LRUCache

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by neejain on 2/4/17.
  */
object LogEntry {
  def insertUpdate(message: String, cache: LRUCache[String, LogEntry]): Unit = {
    val le = cache.get(message).getOrElse(LogEntry(UUID.randomUUID().clockSequence(), ListBuffer[Long]()))
    le.timestamps.append(System.currentTimeMillis())
    cache.put(message, le)
  }
}

case class LogEntry(id: Int, timestamps: mutable.ListBuffer[Long])

//    cache.get(message).getOrElse()
//    if (messageMap.contains(message)) {
//      messageMap.get(message).getOrElse(LogEntry(UUID.randomUUID().clockSequence(), ListBuffer[Long]())).timestamps.append(System.currentTimeMillis())
//    }

