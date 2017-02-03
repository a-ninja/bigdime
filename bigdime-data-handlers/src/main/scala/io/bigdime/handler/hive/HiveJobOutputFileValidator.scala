package io.bigdime.handler.hive

import java.io.IOException

import io.bigdime.alert.LoggerFactory
import io.bigdime.core.commons.AdaptorLogger
import io.bigdime.libs.hdfs.{WebHdfsException, WebHdfsReader}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

/**
  * Created by neejain on 2/1/17.
  */
object HiveJobOutputFileValidator {
  val logger = new AdaptorLogger(LoggerFactory.getLogger(classOf[HiveJobOutputFileValidator]))
}

@Component
@Scope("prototype")
class HiveJobOutputFileValidator {

  import HiveJobOutputFileValidator.logger

  @Autowired private val webHdfsReader: WebHdfsReader = null

  /**
    * Check if the file/directory specified by filePath exists in HDFS.
    *
    * @param filePath absolute hdfs path, without /webhdfs/{version} prefix
    * @return true if the file exists, false otherwise
    * @throws IOException
    * @throws WebHdfsException
    */
  @throws[IOException]
  @throws[WebHdfsException]
  def validateOutputFile(filePath: String): Boolean = try {
    webHdfsReader.getFileStatus(filePath) != null
  } catch {
    case ex: WebHdfsException => {
      if (ex.statusCode == 404) {
        logger.info("validateOutputFile", "_message=\"file not found in hdfs, returning false\" filePath={} error={}", filePath, ex.getMessage)
        false
      }
      else throw ex
    }
  }
}
