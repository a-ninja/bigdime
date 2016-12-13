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
object SwiftClient {
  private val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[SwiftClient]))
}


@Component
@Scope("prototype")
case class SwiftClient() {

  import SwiftClient.logger

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
    storedObject
  }
}