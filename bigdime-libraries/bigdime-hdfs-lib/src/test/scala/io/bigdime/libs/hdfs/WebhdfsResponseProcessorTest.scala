package io.bigdime.libs.hdfs

import java.io.ByteArrayInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import org.testng.annotations.Test


/**
  * Created by neejain on 3/11/17.
  */
@Test(singleThreaded = true)
class WebhdfsResponseProcessorTest {


  @Test
  def testJson = {

    val str = "{\"name\":\"n\"}"
    var stream = new ByteArrayInputStream(str.getBytes)
    val objectMapper = new ObjectMapper
    val v = objectMapper.readValue(stream, classOf[Object])

    //    val v = json[Object](stream, classOf[Object])
    //    val v = json[Class[Object]](new ByteArrayInputStream(str.getBytes))
    println(v)

    //    println(classOf[Object])
    //    println(java.lang.Class[Object])


  }

}
