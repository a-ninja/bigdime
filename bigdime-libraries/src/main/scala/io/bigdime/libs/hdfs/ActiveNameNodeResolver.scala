package io.bigdime.libs.hdfs

import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.LoggerFactory

/**
  * Created by neejain on 1/13/17.
  */
object ActiveNameNodeResolver {

  val logger = LoggerFactory.getLogger(getClass)
  var activeHostName: String = null

  private def stringToList(str: String, delimiter: String = ","): List[String] = {
    str match {
      case null => throw new IllegalArgumentException("null or empty value not allowed for str parameter")
      case _: String => str.split(delimiter).map(token => token.trim).toList
    }
  }

  /**
    * If activeHost is null or if the cachec activeHostName is different from the activeHost arg, a new activeHost is picked from the hosts arg
    *
    * @param hosts      available host names, separated by some delimiter(default is comma)
    * @param activeHost current activeHost
    * @return
    */
  def rotateHost(hosts: String, activeHost: String): String = {
    this.synchronized {

      hosts match {
        case null => throw new IllegalArgumentException("null or empty value not allowed for hosts parameter")
        case _: String =>
          if (hosts.trim.isEmpty) throw new IllegalArgumentException("null or empty value not allowed for hosts parameter")

          val hostList = stringToList(hosts)
          activeHostName match {
            case null =>
              activeHostName = hostList.head
              logger.info("_message=\"first time setup, hostname={}\"", activeHostName)
              activeHostName
            case _: String =>
              if (activeHost == null || !activeHostName.equalsIgnoreCase(activeHost)) activeHostName
              else {
                val in = hostList.indexOf(activeHost)
                val atomicIndex = new AtomicInteger(hostList.indexOf(activeHost))
                val size = hostList.size
                val indexTail = atomicIndex.incrementAndGet() % size

                val index = if (indexTail < 0) indexTail + size
                else indexTail
                activeHostName = hostList(index)
                logger.info("_message=\"rotated hostname={}\"", activeHostName)
                activeHostName
              }
          }
      }
    }
  }
}
