package io.bigdime.handler.swift

import java.util
import java.util.concurrent.{Callable, ExecutionException, Executors, FutureTask}

import io.bigdime.core.ActionEvent.Status
import io.bigdime.core.{ActionEvent, AdaptorConfigurationException}
import io.bigdime.core.constants.ActionEventHeaderConstants
import io.bigdime.core.handler.HandlerContext
import io.bigdime.libs.client.SwiftClient
import org.javaswift.joss.model.{Container, DirectoryOrObject, StoredObject}
import org.mockito.Matchers._
import org.mockito.Mockito
import org.springframework.test.util.ReflectionTestUtils
import org.testng.Assert
import org.testng.annotations.Test

class SwiftTouchFileWriterHandlerScalaTest {
  @Test
  @throws[AdaptorConfigurationException]
  def testBuild() {
    val properties = new java.util.HashMap[String, AnyRef]
    properties.put(SwiftWriterHandlerConstants.USER_NAME, "unit-swift-user-name")
    properties.put(SwiftWriterHandlerConstants.PASSWORD, "unit-swift-password")
    properties.put(SwiftWriterHandlerConstants.AUTH_URL, "unit-swift-auth-url")
    properties.put(SwiftWriterHandlerConstants.TENANT_ID, "unit-swift-tenant-id")
    properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name")
    properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name")
    properties.put(SwiftWriterHandlerConstants.CONTAINER_NAME, "unit-swift-container-name")
    properties.put(SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN, "\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/([\\w\\-]+)\\/(\\w+)\\/([\\w\\.]+)$")
    properties.put(SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN, "$1__$2/$3")
    val swiftByteWriterHandler = new SwiftByteWriterHandler
    swiftByteWriterHandler.setPropertyMap(properties)
    swiftByteWriterHandler.build()
  }

  @Test(threadPoolSize = 1)
  @throws[Throwable]
  def testProcessWithNoEventsInContext() {
    val writeData = "unit-test-testProcess"
    val contextEventList = new util.ArrayList[ActionEvent]
    val returnStatus = setupHandler(writeData, contextEventList, getDefaultProperties)
    Assert.assertEquals(returnStatus.getStatus, Status.BACKOFF_NOW)
    val actionEvents = returnStatus.getEventList
    Assert.assertNull(actionEvents)
  }

  @Test(threadPoolSize = 1) //disable after rewriting to scala @throws[Throwable]
  def testProcessWithOneEventInContext() {
    val writeData = "unit-test-testProcess"
    val contextEventList = getTestEvents(1)
    val returnStatus = setupHandler(writeData, contextEventList, getDefaultProperties)
    Assert.assertEquals(returnStatus.getStatus, Status.READY)
    val actionEvents = returnStatus.getEventList
    Assert.assertNull(actionEvents)
  }

  @Test(threadPoolSize = 1, enabled = false) //disable after rewriting to scala @throws[Throwable]
  def testProcessWithEventsInContextAndNotLastHandler() {
    val writeData = "unit-test-testProcess"
    val contextEventList = getTestEvents(1)
    val properties = getDefaultProperties
    properties.remove(ActionEventHeaderConstants.LAST_HANDLER_IN_CHAIN)
    val returnStatus = setupHandler(writeData, contextEventList, properties)
    Assert.assertEquals(returnStatus.getStatus, Status.READY)
    val actionEvents = returnStatus.getEventList
    Assert.assertNotNull(actionEvents)
  }

  @Test(threadPoolSize = 1) //disable after rewriting to scala @throws[Throwable]
  def testProcessWithEventsInContext() {
    val writeData = "unit-test-testProcess"
    val contextEventList = getTestEvents(2)
    val returnStatus = setupHandler(writeData, contextEventList, getDefaultProperties)
    Assert.assertEquals(returnStatus.getStatus, Status.CALLBACK)
    val actionEvents = returnStatus.getEventList
    System.out.println("actionEvents=" + actionEvents)
    Assert.assertNull(actionEvents)
  }

  private[swift] class ReturnStatus {
    private var status:ActionEvent.Status = null
    private var eventList:util.List[ActionEvent] = new util.ArrayList[ActionEvent]

    def getStatus: ActionEvent.Status = status

    def setStatus(status: ActionEvent.Status) {
      this.status = status
    }

    def getEventList: util.List[ActionEvent] = eventList

    def setEventList(eventList: util.List[ActionEvent]) {
      this.eventList = eventList
    }
  }

  @throws[Throwable]
  def setupHandler(writeData: String, contextEventList: util.List[ActionEvent], properties: util.Map[String, AnyRef]): ReturnStatus = {
    val futureTask = new FutureTask[ReturnStatus](new Callable[ReturnStatus]() {
      @throws[Exception]
      def call: ReturnStatus = {
        val swiftTouchFileWriterHandler = createSwiftTouchFileWriterHandler(properties)
        ReflectionTestUtils.setField(swiftTouchFileWriterHandler, "initialized", true)
        val context = HandlerContext.get
        val journal = new SwiftWriterHandlerJournal
        context.setJournal(swiftTouchFileWriterHandler.getId, journal)
        context.setEventList(contextEventList)
        val status = swiftTouchFileWriterHandler.process
        val returnStatus = new ReturnStatus
        returnStatus.setStatus(status)
        returnStatus.setEventList(context.getEventList)
        System.out.println("context in thread=" + context + "," + context.getEventList)
        returnStatus
      }
    })
    val executorService = Executors.newFixedThreadPool(1)
    executorService.execute(futureTask)
    try
      futureTask.get

    catch {
      case ex: ExecutionException => {
        throw ex.getCause
      }
    }
  }

  private def getDefaultProperties = {
    val properties = new java.util.HashMap[String, AnyRef]
    properties.put(SwiftWriterHandlerConstants.USER_NAME, "unit-swift-user-name")
    properties.put(SwiftWriterHandlerConstants.PASSWORD, "unit-swift-password")
    properties.put(SwiftWriterHandlerConstants.AUTH_URL, "unit-swift-auth-url")
    properties.put(SwiftWriterHandlerConstants.TENANT_ID, "unit-swift-tenant-id")
    properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name")
    properties.put(SwiftWriterHandlerConstants.TENANT_NAME, "unit-swift-tenant-name")
    properties.put(SwiftWriterHandlerConstants.CONTAINER_NAME, "unit-swift-container-name")
    properties.put(ActionEventHeaderConstants.LAST_HANDLER_IN_CHAIN, "true")
    properties.put(SwiftWriterHandlerConstants.INPUT_FILE_PATH_PATTERN, "\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/\\w+\\/([\\w\\-]+)\\/(\\w+)\\/([\\w\\.]+)$")
    properties.put(SwiftWriterHandlerConstants.OUTPUT_FILE_PATH_PATTERN, "touchfiles/$1__$2")
    properties.put(SwiftWriterHandlerConstants.FILE_PATH_PREFIX_PATTERN, "$1__$2/")
    properties.put(SwiftWriterHandlerConstants.FILE_PATH_PATTERN, "$1__$2/$3")
    properties
  }

  @throws[AdaptorConfigurationException]
  private def createSwiftTouchFileWriterHandler(properties: util.Map[String, AnyRef]) = {
    val mockContainer = Mockito.mock(classOf[Container])
    val mockSwiftDirListing = new util.ArrayList[DirectoryOrObject]
    val dirOrObject1 = Mockito.mock(classOf[DirectoryOrObject])
    val storedObject1 = Mockito.mock(classOf[StoredObject])
    Mockito.when(dirOrObject1.getName).thenReturn("20160101__entityName/fileName01.ext")
    Mockito.when(dirOrObject1.getAsObject).thenReturn(storedObject1)
    Mockito.when(storedObject1.getBareName).thenReturn("fileName01.ext")
    val dirOrObject2 = Mockito.mock(classOf[DirectoryOrObject])
    val storedObject2 = Mockito.mock(classOf[StoredObject])
    Mockito.when(dirOrObject2.getName).thenReturn("20160101__entityName/fileName02.ext")
    Mockito.when(dirOrObject2.getAsObject).thenReturn(storedObject2)
    Mockito.when(storedObject2.getBareName).thenReturn("fileName02.ext")
    mockSwiftDirListing.add(dirOrObject1)
    mockSwiftDirListing.add(dirOrObject2)
    Mockito.when(mockContainer.listDirectory(anyString, anyChar, anyString, anyInt)).thenReturn(mockSwiftDirListing)
    val storedObject = Mockito.mock(classOf[StoredObject])
    Mockito.when(mockContainer.getObject(anyString)).thenReturn(storedObject)
    val swiftTouchFileWriterHandler = new SwiftTouchFileWriterHandler
    swiftTouchFileWriterHandler.setPropertyMap(properties)
    swiftTouchFileWriterHandler.build()
    val mockSwiftClient = Mockito.mock(classOf[SwiftClient])
    Mockito.when(mockSwiftClient.container).thenReturn(mockContainer)
    Mockito.when(mockSwiftClient.write(anyString, any(classOf[Array[Byte]]))).thenReturn(storedObject)
    //		ReflectionTestUtils.setField(mockSwiftClient, "container", mockContainer);
    ReflectionTestUtils.setField(swiftTouchFileWriterHandler, "swiftClient", mockSwiftClient)
    swiftTouchFileWriterHandler
  }

  private def getTestEvents(num: Int) = {
    val contextEventList = new util.ArrayList[ActionEvent](num)
    var i = 0
    while (i < num) {
      {
        val event = new ActionEvent
        event.setBody(("/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName01.ext\n/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName0" + num + ".ext").getBytes)
        event.getHeaders.put(ActionEventHeaderConstants.SOURCE_FILE_NAME, "/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName01.ext")
        contextEventList.add(event)
      }
      {
        i += 1; i - 1
      }
    }
    contextEventList
  }
}
