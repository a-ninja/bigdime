package io.bigdime.core.commons

import java.util.Collection

/**
  * Created by neejain on 2/13/17.
  */
object CollectionUtil {
  def getSize(collection: Collection[_]): Int = {
    // PECS
    if (collection == null) return -1
    collection.size
  }

  def isEmpty(collection: Collection[_]): Boolean = collection == null || collection.isEmpty

  def isNotEmpty(collection: Collection[_]): Boolean = !isEmpty(collection)
}
