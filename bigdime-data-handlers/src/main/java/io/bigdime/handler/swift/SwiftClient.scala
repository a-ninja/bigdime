package io.bigdime.handler.swift

import java.io.InputStream

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.commons.AdaptorLogger
import io.bigdime.handler.webhdfs.WebHDFSReaderHandler
import org.javaswift.joss.client.factory.{AccountConfig, AccountFactory}
import org.javaswift.joss.model.Container
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by neejain on 12/10/16.
  */
@Component
@Scope("prototype")
case class SwiftClient() {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[WebHDFSReaderHandler]))
  @Value("${swift.user.name}")
  private val username: String = null

  @Value("${swift.password}")
  private val password: String = null

  @Value("${swift.auth.url}")
  private val authUrl: String = null

  @Value("${swift.tenant.id}")
  private val tenantId: String = null

  @Value("${swift.tenant.name}")
  private val tenantName: String = null

  @Value("${swift.container.name}")
  private val containerName: String = null


  private val containers = scala.collection.mutable.Map[Thread, Container]()

  def container: Container = {
//    val l = List[String]("")
//    l
    containers.get(Thread.currentThread()) match {
      case x: Some[Container] => {
        logger.debug("got container", "get container")
        x.get
      }
      case _ => {
        logger.debug("Initialized container", "initialized container")
        val tempContainer = {
          val accountConfig = new AccountConfig
          accountConfig.setUsername(username)
          accountConfig.setPassword(password)
          accountConfig.setAuthUrl(authUrl)
          accountConfig.setTenantId(tenantId)
          accountConfig.setTenantName(tenantName)
          val account = new AccountFactory(accountConfig).createAccount()
          val c = account.getContainer(containerName)
          containers.put(Thread.currentThread(), c)
          c
        }
        tempContainer
      }
    }
  }

  def write(targetPath: String, data: InputStream) = {
    val storedObject = container.getObject(targetPath)
    storedObject.uploadObject(data)
  }
}

object Client {
  def main(args: Array[String]): Unit = {
    println("main")

    val mb = mutable.Buffer[Int](6,7,8)
    mb.foreach(x=>println(x, mb.remove(0)))
//    for (m <- mb) {
//      println(m)
//      mb.remove(0)
//    }
//    val l= scala.collection.mutable.ListBuffer[Int](1,2,3,4,5,6)
//    val l= scala.collection.mutable.ListBuffer[Int](1,2,3,4,5,6)
//    val lst:List[Int] = List[Int](1,2,3)
//
//    val lb:scala.collection.mutable.ListBuffer[Int] = ListBuffer(1,2,3)
//    lb.append()
//
//    l.foreach(x=> println(l.remove(0)))
//
//    println(l.mkString(","))
  }
}