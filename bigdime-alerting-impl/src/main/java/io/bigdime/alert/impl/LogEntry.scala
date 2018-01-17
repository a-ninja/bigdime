package io.bigdime.alert.impl

import scala.collection.mutable

/**
  * Created by neejain on 2/4/17.
  */
case class LogEntry(id: Long, timestamps: mutable.ListBuffer[Long])