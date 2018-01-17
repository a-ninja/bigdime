package io.bigdime.core.commons

import java.util
import java.util.Map
import java.util.Properties

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
  * Created by neejain on 1/5/17.
  */
object PropertyHelper {
  def getInstance = PropertyHelper

  def getIntProperty(propertyMap: Map[_ <: String, _], name: String): Int = getIntProperty(propertyMap.get(name))

  /**
    * Get the property from propertyMap return an integer value for it.
    *
    * @param propertyMap
    * map to look for the property in
    * @param name
    * name of the property to look for
    * @param defaultValue
    * value to be returned in case property with name argument is
    * not present in the map
    * @return integer value of the property if the property is found and has a
    *         value in integer range. Returns value specified by defalutValue
    *         argument if the property with name argument doesn't exist.
    * @throws NumberFormatException
    * if the property's value cannot be parsed as an integer.
    */
  def getIntProperty(propertyMap: Map[_ <: String, _], name: String, defaultValue: Int): Int = getIntProperty(propertyMap.get(name), defaultValue)

  /**
    * Get the integer value of Object value and return the same. If the value
    * is null or is not a number, the default value is returned.
    *
    * @param value
    * Object with integer value
    * @param defaultValue
    * value to be returned if the value argument is null or not a
    * number
    * @return
    */
  def getIntProperty(value: Any, defaultValue: Int): Int =
    tryOrDefaultValue(getIntProperty, value, defaultValue)

  /**
    * Get the integer value of Object value and return the same. If the value
    * is null or is not a number, an exception is thrown.
    *
    * @param value
    * Object with integer value
    */
  def getIntProperty(value: Any): Int = tryOrFail(integerValueOfWrapper, value)(() => throw new RuntimeException("value is null"))

  /**
    * Get the long value of Object value and return the same. If the value is
    * null or is not a number, the default value is returned.
    *
    * @param value
    * Object with long value
    * @param defaultValue
    * value to be returned if the value argument is null or not a
    * number
    * @return
    */
  def getLongProperty(value: Any, defaultValue: Long): Long =
    tryOrDefaultValue(getLongProperty, value, defaultValue)


  /**
    * Get the long value of Object value and return the same. If the value is
    * null or is not a number, an exception is thrown.
    *
    * @param value
    * Object with long value
    */
  def getLongProperty(value: Any): Long = tryOrFail(longValueOfWrapper, value)(() => throw new RuntimeException("value is null"))

  /**
    * Get the property from propertyMap return a long value for it.
    *
    * @param propertyMap
    * map to look for the property in
    * @param name
    * name of the property to look for
    * @param defaultValue
    * value to be returned in case property with name argument is
    * not present in the map
    * @return long value of the property if the property is found and has a
    *         value in long range. Returns value specified by defalutValue
    *         argument if the property with name argument doesn't exist.
    * @throws NumberFormatException
    * if the property's value cannot be parsed as a long.
    */
  def getLongProperty(propertyMap: Map[_ <: String, _], name: String, defaultValue: Long): Long = {
    getLongProperty(propertyMap.get(name), defaultValue)
  }

  /**
    * Get the property from propertyMap return it's value.
    *
    * @param propertyMap
    * map to look for the property in
    * @param name
    * name of the property to look for
    * @return value of the property if the property is found, null otherwise
    */
  def getStringProperty(propertyMap: Map[_ <: String, _], name: String): String = {
    val value = propertyMap.get(name)
    if (value == null) null else String.valueOf(value)
  }

  /**
    * Get the property from propertyMap return it's value,if it not found
    * return the default value
    *
    * @param propertyMap
    * map to look for the property in
    * @param name
    * name of the property to look for
    * @return value of the property if the property is found, null otherwise
    */
  def getStringProperty(propertyMap: Map[_ <: String, _], name: String, defaultValue: String): String = {
    Option(getStringProperty(propertyMap, name)).getOrElse(defaultValue)
  }

  def getBooleanProperty(propertyMap: Map[_ <: String, _], name: String): Boolean = {
    propertyMap.get(name) match {
      case str: String => java.lang.Boolean.parseBoolean(String.valueOf(str))
      case _ => false
    }
  }

  @SuppressWarnings(Array("rawtypes")) def getMapProperty(propertyMap: Map[_ <: String, _], name: String): Map[String, String] = {
    val value = propertyMap.get(name)
    if (value == null || value == "null") return null
    if (value.isInstanceOf[Map[String, String]]) value.asInstanceOf[Map[String, String]]
    else throw new IllegalArgumentException(name + " field in given map is not of type Map, rather is of type: " + value.getClass)
  }

  def redeemTokensFromAppProperties(properties: Map[_ >: String, _ >: Any], applicationProperties: Properties) {
    import scala.collection.JavaConversions._
    for (property <- properties.entrySet) {
      if (property.getValue.isInstanceOf[String]) {
        val propValue = property.getValue.toString
        val newValue = StringHelper.getInstance.redeemToken(propValue, applicationProperties)
        if (!(propValue.equals(newValue))) property.setValue(newValue)
      }
      else if (property.getValue.isInstanceOf[Map[_, _]]) redeemTokensFromAppProperties(property.getValue.asInstanceOf[Map[String, _ >: Any]], applicationProperties)
    }
  }

  def getStringPropertyFromPropertiesOrSrcDesc(defaultMap: Map[_ <: String, _], overrideMap: Map[_ <: String, _], propertyName: String, defaultValue: String): String = {
    getPropertyFromPropertiesOrSrcDesc(getStringProperty, defaultMap, overrideMap, propertyName, defaultValue)
  }

  def getIntPropertyFromPropertiesOrSrcDesc(defaultMap: Map[_ <: String, _], overrideMap: Map[_ <: String, _], propertyName: String, defaultValue: Int): Int = {
    getPropertyFromPropertiesOrSrcDesc(getIntProperty, defaultMap, overrideMap, propertyName, defaultValue)
  }

  def getPropertyFromPropertiesOrSrcDesc[T](f: (Map[_ <: String, _], String, T) => T, defaultMap: Map[_ <: String, _], overrideMap: Map[_ <: String, _], propertyName: String, defaultValue: T): T = {

    var propertyValue = f(defaultMap, propertyName, defaultValue)
    overrideMap match {
      case m: Map[String, _] => f(m, propertyName, propertyValue)
      case _ => propertyValue
    }
  }


  def tryOrDefaultValue[T](f: (Any) => T, arg: Any, defaultValue: T) = {
    try {
      f(arg)
    } catch {
      case _: Exception => defaultValue
    }
  }

  def tryOrFail[T](f: (Any) => T, arg: Any)(fail: () => T) = {
    if (arg != null) f(arg) else fail()
  }

  def integerValueOfWrapper(v: Any) = v.toString.toInt

  def longValueOfWrapper(v: Any) = v.toString.toLong


  //  def main(args: Array[String]): Unit = {
  //    val v = tryOrDefaultValue(getLongProperty, "f10", 0)
  //    println(v)
  //    val x = 1
  //    val y = tryOrFail(integerValueOfWrapper, x)(() => throw new RuntimeException("value is null"))
  //
  //    val pMap = new util.HashMap[String, Object]()
  //    pMap.put("key", "null")
  //    val bool = getBooleanProperty(pMap, "key")
  //    println(bool)
  //        val y = ifNotNull(x)(() => Integer.valueOf(x))(() => throw new RuntimeException("value is null"))
  //        println(y)
  //  }
}
