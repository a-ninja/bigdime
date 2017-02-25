package io.bigdime.alert.impl.swift

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.{InetAddress, NetworkInterface, SocketException, UnknownHostException}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, FutureTask}

import io.bigdime.alert.impl.{AbstractLogger, LogEntry, LogMessageBuilder}
import io.bigdime.alert.{AlertMessage, Logger}
import io.bigdime.util.{LRUCache, RetryUntilSuccessful, TryWithCloseable}
import org.javaswift.joss.client.factory.{AccountConfig, AccountFactory}
import org.joda.time.format.DateTimeFormat
import org.slf4j.helpers.MessageFormatter
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

/**
  * Created by neejain on 2/2/17.
  */
object SwiftLogger {

  private val loggerMap = new ConcurrentHashMap[String, SwiftLogger]
  val EMPTYSTRING = ""
  private val APPLICATION_CONTEXT_PATH = "META-INF/application-context-monitoring.xml"
  val SWIFT_USER_NAME_PROPERTY = "${swift.user.name}"
  val SWIFT_PASSWORD_PROPERTY = "${swift.password}"
  val SWIFT_AUTH_URL_PROPERTY = "${swift.auth.url}"
  val SWIFT_TENANT_ID_PROPERTY = "${swift.tenant.id}"
  val SWIFT_TENANT_NAME_PROPERTY = "${swift.tenant.name}"
  val SWIFT_PREFERRED_REGION = "${swift.preferred.region}"
  val SWIFT_ALERT_CONTAINER_NAME_PROPERTY = "${swift.alert.container.name}"
  val SWIFT_ALERT_LEVEL_PROPERTY = "${swift.alert.level}"
  val SWIFT_BUFFER_SIZE_PROPERTY = "${swift.debugInfo.bufferSize}"
  val IP_INIT_VAL = "10"
  private var hostName = "UNKNOWN"
  private var hostIp: String = _
  private val msgIdCounter = new AtomicLong(System.currentTimeMillis())
  private var executorService: ExecutorService = Executors.newFixedThreadPool(1)

  //  logger.executorService = Executors.newFixedThreadPool(1)
  try {
    hostName = InetAddress.getLocalHost.getHostName
    NetworkInterface.getNetworkInterfaces.foreach(nwInterface => {
      Option(nwInterface.getInetAddresses.takeWhile(inetAddr => inetAddr.getHostAddress.startsWith(IP_INIT_VAL))) match {
        case Some(iter: Iterator[InetAddress]) => if (iter.hasNext) hostIp = iter.next.getHostAddress
      }
    })
  }
  catch {
    case e: UnknownHostException => System.err.print("UnknownHostException: The host name is " + hostName + ", exception=" + e.toString)
      e.printStackTrace(System.err)
    case e1: SocketException => System.err.print("SocketException: Error while connecting to " + hostName + " host" + ", hostIp=" + hostIp + ", exception=" + e1.toString)
      e1.printStackTrace(System.err)
    case e: Exception => System.err.print("Error while connecting to " + hostName + " host" + ", hostIp=" + hostIp + ", exception=" + e.toString)
      e.printStackTrace(System.err)
  }

  def getLogger(loggerName: String): SwiftLogger = {
    var logger = loggerMap.get(loggerName)
    if (logger == null) {
      logger = SwiftLogger()
      loggerMap.put(loggerName, logger)

      val resp = TryWithCloseable[ClassPathXmlApplicationContext, Unit](new ClassPathXmlApplicationContext(APPLICATION_CONTEXT_PATH))(
        (context) => {
          val ts = List[Class[_ <: Throwable]](classOf[Throwable])
          RetryUntilSuccessful(ts)(() => {
            val config = new AccountConfig
            val beanFactory = context.getBeanFactory
            val _containerName = context.getBeanFactory.resolveEmbeddedValue(SWIFT_ALERT_CONTAINER_NAME_PROPERTY)
            config.setPreferredRegion(context.getBeanFactory.resolveEmbeddedValue(SWIFT_PREFERRED_REGION))
            config.setUsername(context.getBeanFactory.resolveEmbeddedValue(SWIFT_USER_NAME_PROPERTY))
            config.setPassword(context.getBeanFactory.resolveEmbeddedValue(SWIFT_PASSWORD_PROPERTY))
            config.setAuthUrl(context.getBeanFactory.resolveEmbeddedValue(SWIFT_AUTH_URL_PROPERTY))
            config.setTenantId(context.getBeanFactory.resolveEmbeddedValue(SWIFT_TENANT_ID_PROPERTY))
            config.setTenantName(context.getBeanFactory.resolveEmbeddedValue(SWIFT_TENANT_NAME_PROPERTY))
            logger.containerName = _containerName
            logger.accountFactory = new AccountFactory(config)
            logger.swiftAlertLevel = context.getBeanFactory.resolveEmbeddedValue(SWIFT_ALERT_LEVEL_PROPERTY)
            try {
              val bufferSize = beanFactory.resolveEmbeddedValue(SWIFT_BUFFER_SIZE_PROPERTY).toLong
              logger.capacity = bufferSize.toLong
              System.out.println("setting buffer size from property as:" + logger.capacity)

            } catch {
              case ex: Exception =>
                logger.capacity = 4 * 1024
                System.out.println("setting default buffer size as:" + logger.capacity + ", due to " + ex.toString)
            }
            if (logger.swiftAlertLevel != null) if (logger.swiftAlertLevel.equalsIgnoreCase("debug")) logger.setDebugEnabled()
            else if (logger.swiftAlertLevel.equalsIgnoreCase("info")) logger.setInfoEnabled()
            else if (logger.swiftAlertLevel.equalsIgnoreCase("warn")) logger.setWarnEnabled()
            System.out.println("swiftAlertContainerName=" + _containerName + ", swiftAlertLevel=" + logger.swiftAlertLevel + ", capacity=" + logger.capacity)
          })
        })
      resp match {
        case Success(_) =>
        case Failure(f) => f.printStackTrace(System.err)
      }
    }
    logger
  }
}

case class SwiftLogger() extends AbstractLogger with Logger {

  import SwiftLogger.{executorService, msgIdCounter}

  private var swiftAlertLevel: String = _

  private var containerName: String = _
  private var capacity: Long = 10 * 1024
  private[swift] val logDtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private var accountFactory: AccountFactory = _


  private def formatter(format: String, o: Object*) = {
    def toNotNull(o: Object*) = {
      if (o != null) o.toArray else Array.emptyObjectArray
    }

    MessageFormatter.arrayFormat(format, toNotNull(o: _*)).getMessage
  }

  def debug(source: String, shortMessage: String, message: String): Unit = {
    if (isDebugEnabled) logDebugInfoToSwift(source, shortMessage, message, "debug")
  }

  def debug(source: String, shortMessage: String, format: String, o: Object*): Unit = {
    if (isDebugEnabled) {
      debug(source, shortMessage, formatter(format, o))
    }
  }

  def info(source: String, shortMessage: String, message: String): Unit = {
    if (isInfoEnabled) logDebugInfoToSwift(source, shortMessage, message, "info")
  }

  def info(source: String, shortMessage: String, format: String, o: Object*): Unit = {
    if (isInfoEnabled) {
      info(source, shortMessage, formatter(format, o))
    }
  }

  def warn(source: String, shortMessage: String, format: String, o: Object*): Unit = {
    if (isWarnEnabled) {
      warn(source, shortMessage, formatter(format, o))
    }
  }

  def warn(source: String, shortMessage: String, message: String): Unit = {
    if (isWarnEnabled) warn(source, shortMessage, message, null.asInstanceOf[Throwable])
  }

  def warn(source: String, shortMessage: String, message: String, t: Throwable): Unit = {
    if (isWarnEnabled) logToSwift(source, shortMessage, message, "warn", t)
  }

  def alert(message: AlertMessage): Unit = {
    alert(message.getAdaptorName, message.getType, message.getCause, message.getSeverity, message.getMessage)
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String): Unit = {
    alert(source, alertType, alertCause, alertSeverity, message, null.asInstanceOf[Throwable])
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, t: Throwable, format: String, o: Object*): Unit = {
    alert(source, alertType, alertCause, alertSeverity, formatter(format, o: _*), null.asInstanceOf[Throwable])
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, _message: String, t: Throwable): Unit = {
    val b = getExceptionByteArray(t)
    val message = if (b != null) _message + " " + new String(b) else _message

    val newMessage = new LogMessageBuilder()
      .withKV("{}", logDtf.print(System.currentTimeMillis()))
      .withKV("{}", "ERROR")
      .withKV("thread={}", Thread.currentThread.getName)
      .withKV("adaptor_name={}", source)
      .withKV("alert_severity={}", alertSeverity)
      .withKV("message_context=\"{}\"", "todo: set context")
      .withKV("alert_code={}", alertType.getMessageCode)
      .withKV("alert_name=\"{}\"", alertType.getDescription)
      .withKV("alert_cause=\"{}\"", alertType.getDescription)
      .withKV("{}", message).build()
    writeToSwift(source, newMessage.getBytes)
  }

  private val baos = new ByteArrayOutputStream

  private def getExceptionByteArray(t: Throwable) = {
    if (t != null) {
      val baos = new ByteArrayOutputStream
      val ps = new PrintStream(baos)
      t.printStackTrace(ps)
      baos.toByteArray
    } else null
  }

  private def logToSwift(source: String, shortMessage: String, message: String, level: String, t: Throwable) = {
    val b = getExceptionByteArray(t)
    val msg = if (b != null) message + " " + new String(b) else message
    logDebugInfoToSwift(source, shortMessage, msg, level)
  }

  private def logDebugInfoToSwift(source: String, shortMessage: String, message: String, level: String): Unit = {

    val messageKey = new LogMessageBuilder().withKV("level={}", level).withKV("message_context={}", shortMessage).withKV("{}", message).build()
    val timestamp = System.currentTimeMillis

    val put = if (cache.contains(messageKey)) {
      //Get the id and write to logs
      val messageId = cache.get(messageKey).get.id
      val newMessage = new LogMessageBuilder()
        .withKV("{}", logDtf.print(timestamp))
        .withKV("msg_id={}", messageId.toString)
        .build()
      cache.get(messageKey).get.timestamps.append(timestamp)
      newMessage.getBytes
    } else {
      val messageId = msgIdCounter.incrementAndGet()
      val newMessage = new LogMessageBuilder()
        .withKV("{}", logDtf.print(timestamp))
        .withKV("level={}", level)
        .withKV("thread={}", Thread.currentThread.getName)
        .withKV("msg_id={}", messageId.toString)
        .withKV("adaptor_name={}", source)
        .withKV("message_context=\"{}\"", shortMessage)
        .withKV("{}", message).build()
      cache.put(messageKey, LogEntry(messageId, ListBuffer(timestamp)))
      newMessage.getBytes
    }

    if (level.equalsIgnoreCase("warn")) {
      writeToSwift(source, put)
    } else {
      val dataTowrite = baos synchronized {
        baos.write(put, 0, put.length)
        if (baos.size >= capacity) {
          val dataTowrite = baos.toByteArray
          baos.reset()
          dataTowrite
        } else null
      }
      if (dataTowrite != null) {
        writeToSwift(source, dataTowrite)
      }
    }
  }

  //  var futureTask: FutureTask[Any] = _

  private def writeToSwift(source: String, dataTowrite: Array[Byte]): Unit = {
    val logTask = SwiftLogTask(accountFactory, containerName, source, dataTowrite)
    val futureTask = new FutureTask[Any](logTask)
    executorService.execute(futureTask)
    //    startHealthcheckThread(futureTask)
  }

  private val cache = LRUCache[String, LogEntry](1000)

  //  protected def startHealthcheckThread(futureTask: FutureTask[Any]): Unit = {
  //    new Thread() {
  //      override def run() {
  //        try {
  //          System.out.println("heathcheck thread for swiftLogger-start")
  //          futureTask.get
  //          System.out.println("heathcheck thread for swiftLogger-end")
  //        } catch {
  //          case e: Exception =>
  //            e.printStackTrace(System.err)
  //        }
  //      }
  //    }.start()
  //  }
}
