package io.bigdime.libs.hdfs

import java.util.concurrent.atomic.AtomicInteger

/**
  * Created by neejain on 3/16/17.
  */

object RoundRobinStrategy {

  private var hostList: List[String] = _
  private val COMMA = ","
  private val atomicIndex = new AtomicInteger(0)

  def setHosts(hosts: String) = {
    if (hostList == null)
      hostList = decodeHostList(hosts)
  }

  /**
    * Keeps track of the last index over multiple dispatches. Each invocation
    * of this method will increment the index by one, overflowing at
    * <code>size</code>.
    */
  def getNextServiceHost: String = {
    val size = hostList.size
    var index = 0
    val indexTail = atomicIndex.getAndIncrement % size
    index = if (indexTail < 0) indexTail + size
    else indexTail
    hostList(index)
  }

  private def decodeHostList(str: String) = {
    (for (host <- str.split(COMMA)) yield host).toList
  }
}