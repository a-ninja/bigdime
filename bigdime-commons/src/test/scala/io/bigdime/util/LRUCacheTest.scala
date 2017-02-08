package io.bigdime.util

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by neejain on 2/7/17.
  */
class LRUCacheTest {

  @Test
  def testCache() = {
    val cache = LRUCache[String, Boolean](4)
    cache.put("1", true)
    Assert.assertTrue(cache.contains("1"))
    Assert.assertEquals(cache.head.key, "1")
    Assert.assertEquals(cache.tail.key, "1")
    cache.put("2", true)
    Assert.assertTrue(cache.contains("1"))
    Assert.assertTrue(cache.contains("2"))
    Assert.assertEquals(cache.head.key, "2")
    Assert.assertEquals(cache.tail.key, "1")
    cache.put("3", true)
    Assert.assertTrue(cache.contains("1"))
    Assert.assertTrue(cache.contains("2"))
    Assert.assertTrue(cache.contains("3"))
    Assert.assertEquals(cache.head.key, "3")
    Assert.assertEquals(cache.tail.key, "1")
    cache.put("4", true)
    Assert.assertTrue(cache.contains("1"))
    Assert.assertTrue(cache.contains("2"))
    Assert.assertTrue(cache.contains("3"))
    Assert.assertTrue(cache.contains("4"))
    Assert.assertEquals(cache.head.key, "4")
    Assert.assertEquals(cache.tail.key, "1")
    cache.put("5", true)
    Assert.assertTrue(cache.contains("2"))
    Assert.assertTrue(cache.contains("3"))
    Assert.assertTrue(cache.contains("4"))
    Assert.assertTrue(cache.contains("5"))
    Assert.assertEquals(cache.head.key, "5")
    Assert.assertEquals(cache.tail.key, "2")

    Assert.assertFalse(cache.contains("1"))

    cache.put("/apps/hdmi-set/sscience/dsbe/2017/02/06/categorydemandfinal/", true)
    Assert.assertTrue(cache.contains("/apps/hdmi-set/sscience/dsbe/2017/02/06/categorydemandfinal/"))
  }
}
