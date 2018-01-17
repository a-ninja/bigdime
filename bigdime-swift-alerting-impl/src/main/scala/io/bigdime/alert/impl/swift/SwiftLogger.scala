package io.bigdime.alert.impl.swift

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.{InetAddress, NetworkInterface, SocketException, UnknownHostException}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, FutureTask}
import javax.sound.midi.ShortMessage

import io.bigdime.alert.{AlertMessage, Logger}
import io.bigdime.util.{TryWithCloseable, TryWithResources}
import org.javaswift.joss.client.factory.{AccountConfig, AccountFactory}
import org.javaswift.joss.model.Container
import org.joda.time.format.DateTimeFormat
import org.slf4j.helpers.MessageFormatter
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
  val SWIFT_ALERT_CONTAINER_NAME_PROPERTY = "${swift.alert.container.name}"
  val SWIFT_ALERT_LEVEL_PROPERTY = "${swift.alert.level}"
  val SWIFT_BUFFER_SIZE_PROPERTY = "${swift.debugInfo.bufferSize}"
  val IP_INIT_VAL = "10"
  private var hostName = "UNKNOWN"
  private var hostIp: String = null
  try {
    hostName = InetAddress.getLocalHost.getHostName
    NetworkInterface.getNetworkInterfaces.foreach(nwInterface => {
      Option(nwInterface.getInetAddresses.takeWhile(inetAddr => inetAddr.getHostAddress.startsWith(IP_INIT_VAL))) match {
        case Some(iter: Iterator[InetAddress]) => if (iter.hasNext) hostIp = iter.next.getHostAddress
      }
    })
  }
  catch {
    case e: UnknownHostException => {
      System.err.print("The host name is " + hostName)
    }
    case e1: SocketException => {
      System.err.print("Error while connecting to " + hostName + " host" + ", hostIp=" + hostIp)
    }
    case e: Exception => System.err.print("Error while connecting to " + hostName + " host" + ", hostIp=" + hostIp)
      e.printStackTrace()
  }

  def getLogger(loggerName: String): SwiftLogger = {
    var logger = loggerMap.get(loggerName)
    if (logger == null) {
      logger = SwiftLogger()
      loggerMap.put(loggerName, logger)

      TryWithCloseable[ClassPathXmlApplicationContext, Unit](new ClassPathXmlApplicationContext(APPLICATION_CONTEXT_PATH))((context) => {
        val config = new AccountConfig
        val beanFactory = context.getBeanFactory
        val containerName = context.getBeanFactory.resolveEmbeddedValue(SWIFT_ALERT_CONTAINER_NAME_PROPERTY)
        config.setUsername(context.getBeanFactory.resolveEmbeddedValue(SWIFT_USER_NAME_PROPERTY))
        config.setPassword(context.getBeanFactory.resolveEmbeddedValue(SWIFT_PASSWORD_PROPERTY))
        config.setAuthUrl(context.getBeanFactory.resolveEmbeddedValue(SWIFT_AUTH_URL_PROPERTY))
        config.setTenantId(context.getBeanFactory.resolveEmbeddedValue(SWIFT_TENANT_ID_PROPERTY))
        config.setTenantName(context.getBeanFactory.resolveEmbeddedValue(SWIFT_TENANT_NAME_PROPERTY))
        val account = new AccountFactory(config).createAccount
        logger.container = account.getContainer(containerName)
        logger.swiftAlertLevel = context.getBeanFactory.resolveEmbeddedValue(SWIFT_ALERT_LEVEL_PROPERTY)
        try {
          val bufferSize = beanFactory.resolveEmbeddedValue(SWIFT_BUFFER_SIZE_PROPERTY).toLong
          logger.capacity = bufferSize.toLong
          System.out.println("setting buffer size from property as:" + logger.capacity)

        } catch {
          case ex: Exception => {
            logger.capacity = 4 * 1024
            System.out.println("setting default buffer size as:" + logger.capacity)
          }
        }
        if (logger.swiftAlertLevel != null) if (logger.swiftAlertLevel.equalsIgnoreCase("debug")) setDebugEnabled(logger)
        else if (logger.swiftAlertLevel.equalsIgnoreCase("info")) setInfoEnabled(logger)
        else if (logger.swiftAlertLevel.equalsIgnoreCase("warn")) setWarnEnabled(logger)
        logger.executorService = Executors.newFixedThreadPool(1)
        System.out.println("swiftAlertContainerName=" + containerName + ", swiftAlertLevel=" + logger.swiftAlertLevel + ", capacity=" + logger.capacity)
      })


    }
    logger
  }

  private def setDebugEnabled(logger: SwiftLogger) {
    logger.debugEnabled = true
    setInfoEnabled(logger)
  }

  private def setInfoEnabled(logger: SwiftLogger) {
    logger.infoEnabled = true
    setWarnEnabled(logger)
  }

  private def setWarnEnabled(logger: SwiftLogger) {
    logger.warnEnabled = true
  }

}

case class SwiftLogger() extends Logger {

  import SwiftLogger._

  private var swiftAlertLevel: String = null
  private var executorService: ExecutorService = null
  private var container: Container = null
  private var capacity: Long = 10 * 1024
  private var debugEnabled = false
  private var infoEnabled = false
  private var warnEnabled = false
  private[swift] val logDtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")


  private def isDebugEnabled: Boolean = debugEnabled

  private def isInfoEnabled: Boolean = infoEnabled

  private def isWarnEnabled: Boolean = warnEnabled

  def debug(source: String, shortMessage: String, message: String) {
    if (isDebugEnabled) logDebugInfoToSwift(source, shortMessage, message, "debug")
  }

  def debug(source: String, shortMessage: String, format: String, o: Object*) {
    if (isDebugEnabled) {
      val ft = MessageFormatter.arrayFormat(format, o.map(o1 => o1.toString).toArray)
      debug(source, shortMessage, ft.getMessage)
    }
  }

  def info(source: String, shortMessage: String, message: String) {
    if (isInfoEnabled) logDebugInfoToSwift(source, shortMessage, message, "info")
  }

  def info(source: String, shortMessage: String, format: String, o: Object*) {
    if (isInfoEnabled) {
      val ft = MessageFormatter.arrayFormat(format, o.map(o1 => o1.toString).toArray)
      info(source, shortMessage, ft.getMessage)
    }
  }

  def warn(source: String, shortMessage: String, format: String, o: Object*) {
    if (isWarnEnabled) {
      val ft = MessageFormatter.arrayFormat(format, o.map(o1 => o1.toString).toArray)
      warn(source, shortMessage, ft.getMessage)
    }
  }

  def warn(source: String, shortMessage: String, message: String) {
    if (isWarnEnabled) warn(source, shortMessage, message, null.asInstanceOf[Throwable])
  }

  def warn(source: String, shortMessage: String, message: String, t: Throwable) {
    if (isWarnEnabled) logToSwift(source, shortMessage, message, "warn", t)
  }

  def alert(message: AlertMessage) {
    alert(message.getAdaptorName, message.getType, message.getCause, message.getSeverity, message.getMessage)
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, message: String) {
    alert(source, alertType, alertCause, alertSeverity, message, null.asInstanceOf[Throwable])
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, format: String, o: Object*) {
    val ft = MessageFormatter.arrayFormat(format, o.map(o1 => o1.toString).toArray)
    alert(source, alertType, alertCause, alertSeverity, ft.getMessage, null.asInstanceOf[Throwable])
  }

  private def assignToArrayAndIncrementIndex(argArray: Array[AnyRef], i: Int, value: AnyRef): Int = {
    argArray(i) = value
    i + 1
  }

  def alert(source: String, alertType: Logger.ALERT_TYPE, alertCause: Logger.ALERT_CAUSE, alertSeverity: Logger.ALERT_SEVERITY, _message: String, t: Throwable) {
    val b = getExceptionByteArray(t)
    val message = if (b != null) _message + " " + new String(b) else _message
    val sb = new StringBuilder
    sb.append("{} ERROR Thread={} adaptor_name=\"{}\" alert_severity=\"{}\" message_context=\"{}\" alert_code=\"{}\" alert_name=\"{}\" alert_cause=\"{}\"").append(" ").append(message).append("\n")
    val argArray = new Array[AnyRef](2 + 6)

    var i = assignToArrayAndIncrementIndex(argArray, 0, logDtf.print(System.currentTimeMillis()))
    i = assignToArrayAndIncrementIndex(argArray, i, Thread.currentThread().getName())
    i = assignToArrayAndIncrementIndex(argArray, i, source)
    i = assignToArrayAndIncrementIndex(argArray, i, alertSeverity)
    i = assignToArrayAndIncrementIndex(argArray, i, "todo: set context")
    i = assignToArrayAndIncrementIndex(argArray, i, alertType.getMessageCode)
    i = assignToArrayAndIncrementIndex(argArray, i, alertType.getDescription)
    i = assignToArrayAndIncrementIndex(argArray, i, alertCause.getDescription)
    val ft = MessageFormatter.arrayFormat(sb.toString, argArray)
    writeToSwift(source, ft.getMessage.getBytes)
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

  private def logToSwift(source: String, shortMessage: String, message: String, level: String, t: Throwable) {
    val b = getExceptionByteArray(t)
    val msg = if (b != null) message + " " + new String(b) else message
    logDebugInfoToSwift(source, shortMessage, msg, level)
  }

  private def logDebugInfoToSwift(source: String, shortMessage: String, message: String, level: String) {
    val sb = new StringBuilder("{} {} {} adaptor_name=\"{}\" message_context=\"{}\"").append(" ").append(message).append("\n")
    val argArray = buildArgArray(level, source, shortMessage, message, "")

    val putStr = MessageFormatter.arrayFormat(sb.toString, argArray).getMessage
    if (putStr != null) {
      if (shallWriteToSwift(argArray, putStr)) {
        val put = putStr.getBytes
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
  }

  var futureTask: FutureTask[AnyRef] = null

  private def writeToSwift(source: String, dataTowrite: Array[Byte]) {
    val logTask = new SwiftLogTask(container, source, dataTowrite)
    futureTask = new FutureTask[AnyRef](logTask)
    executorService.execute(futureTask)
    startHealthcheckThread()
  }

  val messgageMap = mutable.Map[Int, Set[String]]()

  private def shallWriteToSwift(args: Array[AnyRef], message: String) = {
    //    val hc = args.drop(1).foldLeft(0)((x, y) => x.toString.hashCode + y.toString.hashCode)
    //    val set = messgageMap.get(hc).getOrElse(Set())
    //    messgageMap.put(hc, set + message)
    //    set.isEmpty || !set.contains(message)
    true
  }

  private def buildArgArray(level: String, source: String, shortMessage: String, format: String, o: Any*) = {
    //    sb.append("{} {} {} adaptor_name=\"{}\" message_context=\"{}\"").append(" ").append(format).append("\n")
    val argArray = new Array[AnyRef](3 + 2 + o.length)
    var j = assignToArrayAndIncrementIndex(argArray, 0, logDtf.print(System.currentTimeMillis))
    j = assignToArrayAndIncrementIndex(argArray, j, level)
    j = assignToArrayAndIncrementIndex(argArray, j, Thread.currentThread.getName)
    j = assignToArrayAndIncrementIndex(argArray, j, source)
    j = assignToArrayAndIncrementIndex(argArray, j, shortMessage)

    var i = j
    for (o1 <- o) {
      argArray(i) = o1.toString
      i += 1
    }
    argArray
    //    val ft = MessageFormatter.arrayFormat(sb.toString, argArray)
    //    ft.getMessage
  }

  protected def startHealthcheckThread() {
    new Thread() {
      override def run() {
        try {
          System.out.print("heathcheck thread for swiftLogger")
          futureTask.get
          System.out.print("heathcheck thread for swiftLogger, future task completed")

        } catch {
          case e: Exception => {
            e.printStackTrace(System.err)
          }
        }
      }
    }.start()
  }
}
