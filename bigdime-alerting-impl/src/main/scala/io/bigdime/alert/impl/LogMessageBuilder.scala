package io.bigdime.alert.impl

import java.io.{ByteArrayOutputStream, PrintStream}

import org.slf4j.helpers.MessageFormatter

import scala.collection.mutable.ArrayBuffer

/**
  * Created by neejain on 2/4/17.
  */
class LogMessageBuilder {

  val sb: StringBuilder = new StringBuilder
  val argArray = new ArrayBuffer[AnyRef]
  var str: String = ""
  var t: String = ""

  def withKV(k: String, v: AnyRef*) = {
    sb.append(k).append(" ")
    v.map(x => argArray.append(x))
    this
  }

  def withValue(v: String) = {
    str = v
    this
  }

  def withThrowable(v: Throwable) = {
    if (v != null) {
      val baos = new ByteArrayOutputStream
      val ps = new PrintStream(baos)
      v.printStackTrace(ps)
      t = new String(baos.toByteArray)
    } else t = ""
    this
  }

  def build() = {
    MessageFormatter.arrayFormat(sb.append(str).append(t).append("\n").toString(), argArray.toArray).getMessage
  }

  /*  private def assignToArrayAndIncrementIndex(argArray: Array[AnyRef], i: Int, value: AnyRef): Int = {
      argArray(i) = value
      i + 1
    }

    private def buildArgArray(level: String, source: String, shortMessage: String, format: String, o: Any*) = {
      //    sb.append("{} {} {} adaptor_name=\"{}\" message_context=\"{}\"").append(" ").append(format).append("\n")
      val argArray = new Array[AnyRef](2 + 2 + o.length)
      //    assignToArrayAndIncrementIndex(argArray, 0, logDtf.print(System.currentTimeMillis))
      var j = assignToArrayAndIncrementIndex(argArray, 0, level)
      j = assignToArrayAndIncrementIndex(argArray, j, Thread.currentThread.getName)
      j = assignToArrayAndIncrementIndex(argArray, j, source)
      j = assignToArrayAndIncrementIndex(argArray, j, shortMessage)

      var i = j
      for (o1 <- o) {
        argArray(i) = o1.toString
        i += 1
      }
      argArray
      //    val ft = MessageFormatter.arrayFormat(sb.toString, argArray)
      //    ft.getMessage
    }*/
}
