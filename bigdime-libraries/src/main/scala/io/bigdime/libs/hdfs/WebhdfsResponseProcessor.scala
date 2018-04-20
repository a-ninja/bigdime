package io.bigdime.libs.hdfs

import java.io.{IOException, InputStream}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.http.HttpResponse
import org.apache.http.client.ResponseHandler

import scala.util.{Failure, Success, Try}

/**
  * Created by neejain on 3/9/17.
  * Abstract implementation of org.apache.http.client.ResponseHandler interface.
  */
trait WebhdfsResponseProcessor[T] extends ResponseHandler[Try[T]] {

  private val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  @throws[JsonProcessingException]
  @throws[IOException]
  def json[R](stream: InputStream, cls: Class[R]): R = {
    objectMapper.readValue(stream, cls)
  }

  protected def handle(response: HttpResponse) = {
    response.getStatusLine.getStatusCode match {
      case 200 | 201 => Success(true)
      case x => Failure(new WebHdfsException(x, response.getStatusLine.getReasonPhrase))
    }
  }

  /**
    * Basic implementation of the handleResponse method. Success case is handled by handleSuccess in the concrete class.
    *
    * @param response
    * @return response object wrapped in scala.util.Success or exception wrapped in scala.util.Failure, in case of a failure
    */
  override def handleResponse(response: HttpResponse): Try[T] = {
    val success = handle(response)
    success match {
      case Success(_) => handleSuccess(response)
      case Failure(e) => Failure(e)
    }
  }

  def handleSuccess(response: HttpResponse): Try[T]
}

/**
  * Handles the response from LISTSTATUS webhdfs command.
  *
  * @param webhdfsFilePath
  */
case class ListStatusResponseHandler(webhdfsFilePath: String) extends WebhdfsResponseProcessor[List[String]] {
  protected def isEmptyFile(fs: FileStatus): Boolean = {
    !(fs.`type` == "FILE" && fs.length > 0)
  }

  override def handleSuccess(response: HttpResponse): Try[List[String]] = {
    val fss: WebHdfsListStatusResponse = json[WebHdfsListStatusResponse](response.getEntity.getContent, classOf[WebHdfsListStatusResponse])
    val fileStatuses = fss.FileStatuses.FileStatus
    Success((for (fs <- fileStatuses if !isEmptyFile(fs)) yield webhdfsFilePath + fs.pathSuffix).toList)
  }
}

/**
  * Handles the response from GETFILESTATUS webhdfs command.
  */
case class FileStatusResponseHandler() extends WebhdfsResponseProcessor[FileStatus] {
  override def handleSuccess(response: HttpResponse): Try[FileStatus] = {
    Success(json[WebHdfsGetFileStatusResponse](response.getEntity.getContent, classOf[WebHdfsGetFileStatusResponse]).FileStatus)
  }
}

/**
  * Hamdles the response from a webhdfs command that results in true/false response.
  */
case class BooleanResponseHandler() extends WebhdfsResponseProcessor[Boolean] {
  override def handleSuccess(response: HttpResponse): Try[Boolean] = Success(true)
}

case class InputStreamResponseHandler() extends WebhdfsResponseProcessor[InputStream] {
  override def handleSuccess(response: HttpResponse): Try[InputStream] = Success(response.getEntity.getContent)
}

case class ChecksumResponseHandler() extends WebhdfsResponseProcessor[HdfsFileChecksum] {
  override def handleSuccess(response: HttpResponse): Try[HdfsFileChecksum] = Success(json[WebHdfsGetFileChecksumResponse](response.getEntity.getContent, classOf[WebHdfsGetFileChecksumResponse]).FileChecksum)
}

case class RedirectLocationHandler() extends WebhdfsResponseProcessor[String] {
  override protected def handle(response: HttpResponse) = {
    response.getStatusLine.getStatusCode match {
      case 307 => Success(true)
      case x => Failure(new WebHdfsException(x, response.getStatusLine.getReasonPhrase))
    }
  }

  override def handleSuccess(response: HttpResponse): Try[String] = {
    val headers = response.getAllHeaders
    val location = headers.filter(h => h.getName.equals("Location")).headOption match {
      case Some(header) => header.getValue
      case _ => null
    }
    Success(location)
  }
}