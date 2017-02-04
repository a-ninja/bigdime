package io.bigdime.util

import java.util

import scala.collection.mutable

/**
  * Created by neejain on 2/3/17.
  */
/**
  *
  */
object LRUCache {
  def main(args: Array[String]): Unit = {
    val c = LRUCache[String, String](10)
  }
}

case class LRUCache[K, V](capacity: Int) {

  case class Node(var pre: Node, var next: Node, var key: K, var value: V)

  val map = mutable.Map[K, Node]()

  val list = new util.LinkedList[K]()
  var head: Node = null
  var tail: Node = null

  def removeNode(node: Node) = {
    if (node.pre != null) node.pre.next = node.next
    if (node.next != null) node.next.pre = node.pre
  }

  def insertNodeOnHead(node: Node) = {
    node.next = head
    if (head != null) head.pre = node
  }

  //  def makeKeyRecent(key: K) = {
  //    list.remove(key)
  //    list.add(key)
  //  }
  //
  //  def resizeList(key: K) = {
  //  }

  /**
    *
    */
  def get(key: K): Option[V] = {
    if (map.contains(key)) {
      val node = map.get(key).get
      removeNode(node)
      insertNodeOnHead(node)
      Some(node.value)
    } else None
  }

  def contains(key: K): Boolean = {
    map.contains(key)
  }

  def put(key: K, value: V) = {
    val node = if (map.contains(key)) {
      val n = map.get(key).get
      n.value = value
      removeNode(n)
      n
    } else {
      val n = Node(null, head, key, value)
      if (map.size >= capacity) {
        val keyToRemove = tail.key
        map - keyToRemove
        removeNode(n)
        map.put(key, n)
      }
      n
    }
    insertNodeOnHead(node)
  }
}
