package io.bigdime.core.commons

import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by neejain on 1/13/17.
  */
object HostNameUtils {
  val logger = LoggerFactory.getLogger(getClass)

  private def stringToList(str: String, delimiter: String = ","): List[String] = {
    str match {
      case null => throw new IllegalArgumentException("null or empty value not allowed for str parameter")
      case _: String => str.split(delimiter).map(token => token.trim).toList
    }
  }

  def rotateHost(hosts: String, activeHost: String): String = {
    hosts match {
      case null => throw new IllegalArgumentException("null or empty value not allowed for hosts parameter")
      case _: String =>
        if (hosts.trim.isEmpty) throw new IllegalArgumentException("null or empty value not allowed for hosts parameter")
        val hostList = stringToList(hosts)
        activeHost match {
          case null =>
            val hName = hostList.head
            logger.info("_message=\"first time setup, hostname={}\"", hName)
            hName

          case _: String =>
            val in = hostList.indexOf(activeHost)
            val atomicIndex = new AtomicInteger(hostList.indexOf(activeHost))
            val size = hostList.size
            val indexTail = atomicIndex.incrementAndGet() % size

            val index = if (indexTail < 0) indexTail + size
            else indexTail
            val hName = hostList(index)
            logger.info("_message=\"rotated hostname={}\"", hName)
            hName
        }
    }
  }
}
