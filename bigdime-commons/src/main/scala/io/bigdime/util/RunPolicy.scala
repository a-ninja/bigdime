package io.bigdime.util

import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Created by neejain on 12/12/16.
  */
trait RunPolicy {
  def apply[T](block: () => T): Option[T]
}

object RunPolicy {
  val MAX_ATTEMPTS: Int = 100
}

case class RetryUntilSuccessful(retriables: List[Class[_ <: Throwable]]) extends RunPolicy {
  override def apply[T](block: () => T): Option[T] = {
    Retry(RunPolicy.MAX_ATTEMPTS, retriables)(block)
  }
}

object Retry {
  val logger = LoggerFactory.getLogger("Retry")
}

case class Retry(maxAttempts: Int, retriables: List[Class[_ <: Throwable]], delay: Long = 3000) extends RunPolicy {

  import Retry.logger

  override def apply[T](block: () => T): Option[T] = {
    var attempt = 0

    var causes = new ListBuffer[Throwable]
    while (true) {
      attempt += 1
      try {
        val ret = Some(block())
        if (attempt > 1) logger.info("ran successfully, recovered from a prev error. attempt={}", attempt)
        return ret
      } catch {
        case e: Exception =>
          logger.warn("code block executed with Exception, attempt={}", attempt, e)
          causes += e
          if (attempt < maxAttempts) {
            for (r <- retriables if (e.getClass.isInstanceOf[r.type])) {
              Thread.sleep(attempt * delay)
            }
          } else throw RetriesExhaustedException(causes.toList)
      }
    }

    throw RetriesExhaustedException(causes.toList)
  }
}

case class RetryAndGiveUp(maxAttempts: Int, retriables: List[Class[_ <: Throwable]], delay: Long = 3000) extends RunPolicy {

  import Retry.logger

  override def apply[T](block: () => T): Option[T] = {
    try {
      Retry(maxAttempts, retriables, delay)(block)
    } catch {
      case e: RetriesExhaustedException => e.causes.foreach(ex => {
        logger.warn("RetryAndGiveUp", ex)
      })
        None
      case e: Throwable => logger.warn("RetryAndGiveUp", e)
        None
    }
  }
}

case class RetriesExhaustedException(causes: List[Throwable]) extends Throwable
