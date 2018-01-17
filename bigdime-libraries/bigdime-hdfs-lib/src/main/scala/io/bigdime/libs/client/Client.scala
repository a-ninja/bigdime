package io.bigdime.libs.client

import java.io.{File, IOException, InputStream}
import java.util.concurrent.TimeUnit

import io.bigdime.util.Retry
import org.javaswift.joss.client.factory.{AccountConfig, AccountFactory}
import org.javaswift.joss.exception.CommandException
import org.javaswift.joss.headers.`object`.{DeleteAfter, ObjectManifest}
import org.javaswift.joss.instructions.UploadInstructions
import org.javaswift.joss.model.{Container, StoredObject}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

trait Client[I, O] {

  /**
    *
    * @param outputDescriptor
    * @param inputStream
    */
  def write(outputDescriptor: I, inputStream: InputStream): O

  def write(outputDescriptor: I, data: Array[Byte]): O

  def write(outputDescriptor: I, fileName: String): O
}

/**
  * Created by neejain on 12/10/16.
  */
object SwiftClient {
  private val logger = LoggerFactory.getLogger(classOf[SwiftClient])

}

@Component
@Scope("prototype")
case class SwiftClient() extends Client[String, StoredObject] {

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
  @Value("${swift.preferred.region}")
  private val preferredRegion: String = null

  private val containers = scala.collection.mutable.Map[Thread, Container]()

  private val ts = List[Class[_ <: Throwable]](classOf[CommandException])
  private val retryUntilSuccessful = Retry(3, ts, delay = 3000, (t) => evictConnectionFromCache, classOf[IOException])

  def container: Container = {
    containers.get(Thread.currentThread()) match {
      case x: Some[Container] => {
        logger.info("container: retrieved from cache")
        x.get
      }
      case _ => {
        logger.info("container: initializing container")
        val tempContainer = {
          val accountConfig = new AccountConfig
          accountConfig.setUsername(username)
          accountConfig.setPassword(password)
          accountConfig.setAuthUrl(authUrl)
          accountConfig.setTenantId(tenantId)
          accountConfig.setTenantName(tenantName)
          accountConfig.setPreferredRegion(preferredRegion)
          val account = new AccountFactory(accountConfig).createAccount()
          val c = account.getContainer(containerName)
          containers.put(Thread.currentThread(), c)
          c
        }
        tempContainer
      }
    }
  }

  private def evictConnectionFromCache() = {
    logger.info("evicting from cache: {}", Thread.currentThread())
    containers.remove(Thread.currentThread())
  }

  protected def uploadInstructionWithExpiration(data: InputStream) = {
    setExpiration(new UploadInstructions(data))
  }

  protected def uploadInstructionWithExpiration(data: Array[Byte]) = {
    setExpiration(new UploadInstructions(data))
  }

  protected def uploadInstructionWithExpiration(fileName: String) = {
    setExpiration(new UploadInstructions(new File(fileName)))
  }

  protected def uploadInstructionWithExpiration(manifest: ObjectManifest) = {
    setExpiration(new UploadInstructions(Array[Byte]()).setObjectManifest(manifest))
  }

  protected def setExpiration(uploadInst: UploadInstructions) = {
    uploadInst.setDeleteAfter(new DeleteAfter(TimeUnit.DAYS.toSeconds(14)))
  }

  override def write(targetPath: String, data: InputStream) = {
    retryUntilSuccessful(() => {
      val storedObject = container.getObject(targetPath)
      storedObject.uploadObject(uploadInstructionWithExpiration(data))
      storedObject
    }).get
  }


  override def write(objectName: String, data: Array[Byte]): StoredObject = {
    retryUntilSuccessful(() => {
      val storedObject = container.getObject(objectName)
      storedObject.uploadObject(uploadInstructionWithExpiration(data))
      logger.info("swiftClient", "_message=\"wrote to swift\" swift_object_name={} object_etag={} object_public_url={}", objectName, storedObject.getEtag, storedObject.getPublicURL)
      storedObject
    }).get
  }

  override def write(objectName: String, fileName: String) = {
    retryUntilSuccessful(() => {
      val storedObject = container.getObject(objectName)
      storedObject.uploadObject(uploadInstructionWithExpiration(fileName))
    })
    ???
  }

}

class SwiftAlertClient(c: Container) extends SwiftClient {
  /**
    *
    * @param segmentName
    * @param message
    */
  def writeSegment(segmentName: String, message: Array[Byte]) {
    val largeObject = c.getObject(segmentName)
    largeObject.uploadObject(uploadInstructionWithExpiration(message))
  }

  /**
    *
    * @param objectName name of the large object
    * @param manifestValue
    */
  def writeManifest(objectName: String, manifestValue: String) {
    val largeObject = c.getObject(objectName)
    val uploadInstructions = uploadInstructionWithExpiration(new ObjectManifest(manifestValue))
    largeObject.uploadObject(uploadInstructions)
  }

}

case class KafkaClient() extends Client[String, String] {
  /**
    *
    * @param outputDescriptor
    * @param inputStream
    */
  override def write(outputDescriptor: String, inputStream: InputStream): String = ???

  override def write(outputDescriptor: String, data: Array[Byte]): String = ???

  override def write(outputDescriptor: String, fileName: String): String = ???
}