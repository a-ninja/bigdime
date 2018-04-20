package io.bigdime.libs.hdfs

import java.util.concurrent.atomic.AtomicInteger

import io.bigdime.libs.hdfs.RoundRobinStrategy.{atomicIndex, decodeHostList}

/**
  * Created by neejain on 3/16/17.
  */

case class RoundRobinStrategy(hosts: String) {
  val hostList: List[String] = decodeHostList(hosts)

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
}

object RoundRobinStrategy {

  private val hostsToStrategyMap = scala.collection.mutable.Map[String, RoundRobinStrategy]()
  //  var hostList: List[String] = _
  private val COMMA = ","
  private val atomicIndex = new AtomicInteger(0)

  def withHosts(hosts: String) = {
    if (!Option(hosts).isDefined || hosts.isEmpty) throw new IllegalArgumentException("hosts cant be null")
    hostsToStrategyMap.get(hosts).getOrElse(hostsToStrategyMap.put(hosts, RoundRobinStrategy(hosts)))
    hostsToStrategyMap.get(hosts).get
  }


  private def decodeHostList(str: String) = {
    (for (host <- str.split(COMMA)) yield host).toList
  }
}