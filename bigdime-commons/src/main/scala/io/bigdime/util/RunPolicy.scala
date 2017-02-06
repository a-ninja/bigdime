package io.bigdime.util

import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

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
        if (attempt > 1) logger.warn("ran successfully, recovered from a prev error. attempt={}", attempt)
        return ret
      } catch {
        case e: Exception =>
          logger.warn("code block executed with Exception, attempt={}/{}", attempt.toString, maxAttempts.toString, e)
          causes += e
          if (retriables.filter(r => r.isInstance(e)).nonEmpty) {
            if (attempt < maxAttempts)
              Thread.sleep(attempt * delay)
            else
              throw RetriesExhaustedException(causes.toList)
          } else
            throw new UnretriableException(e)
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

case class UnretriableException(cause: Throwable) extends Throwable

object TryWithResources {
  val logger = LoggerFactory.getLogger("TryWithResources")

  def apply[A, B](resource: A)(clean: A => Unit)(code: A => B): Try[B] = {
    try {
      Success(code(resource))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      try {
        if (resource != null)
          clean(resource)
      } catch {
        case e: Exception => TryWithResources.logger.warn("error while trying to cleanup the resource", e)
      }
    }
  }
}

object TryWithCloseable {
  def apply[A <: AutoCloseable, B](resource: A)(code: A => B): Try[B] = {
    try {
      Success(code(resource))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      try {
        if (resource != null)
          resource.close()
      } catch {
        case e: Exception => TryWithResources.logger.warn("error while trying to close the resource", e)
      }
    }
  }
}