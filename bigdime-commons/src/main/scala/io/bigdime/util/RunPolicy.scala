package io.bigdime.util

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by neejain on 12/12/16.
  */
trait RunPolicy {
  def apply[T](block: () => T): Option[T]
}

object RunPolicy {
  val MAX_ATTEMPTS: Int = 100

  def noop(t: Throwable): Unit = {}
}

//final case class RetryAndHandleUntilSuccessful(retriables: List[Class[_ <: Throwable]], handle: () => Unit) extends RunPolicy {
//  override def apply[T](block: () => T): Option[T] = {
//    Retry(RunPolicy.MAX_ATTEMPTS, retriables, handle = handle)(block)
//  }
//}

final case class RetryUntilSuccessful(retriables: List[Class[_ <: Throwable]], handle: (Throwable) => Unit = RunPolicy.noop) extends RunPolicy {
  override def apply[T](block: () => T): Option[T] = {
    Retry(RunPolicy.MAX_ATTEMPTS, retriables, handle = handle)(block)
  }
}

//not tested
//final case class RetryUntilElapsed(maxElapsed: Long, retriables: List[Class[_ <: Throwable]]) extends RunPolicy {
//  override def apply[T](block: () => T): Option[T] = {
//    val startTime = System.currentTimeMillis()
//    var endTime = 0l
//    var causes = List[Throwable]()
//
//    do {
//      try {
//        var elapsed = 0l
//        return Retry(1, retriables, noop)(block)
//      } catch {
//        case e: RetriesExhaustedException =>
//          endTime = System.currentTimeMillis()
//          causes = e :: causes
//          if (endTime - startTime > maxElapsed)
//            throw RetriesExhaustedException(causes.toList)
//      }
//
//    } while (endTime - startTime < maxElapsed)
//    throw RetriesExhaustedException(causes.toList)
//  }
//}

object Retry {
  val logger = LoggerFactory.getLogger("Retry")
}


final case class Retry(maxAttempts: Int, retriables: List[Class[_ <: Throwable]], delay: Long = 3000, handle: (Throwable) => Unit = RunPolicy.noop, toThrow: Class[_ <: Throwable] = classOf[RetriesExhaustedException]) extends RunPolicy {

  import Retry.logger

  override def apply[T](block: () => T): Option[T] = {
    var attempt = 0

    var causesHash = Set[String]()
    var causes = List[Throwable]()
    while (true) {
      attempt += 1
      try {
        val ret = Some(block())
        if (attempt > 1) logger.warn("ran successfully, recovered from a prev error. attempt={}", attempt)
        return ret
      } catch {
        case e: Exception =>
          logger.warn("code block executed with Exception, attempt={}/{}", attempt.toString, maxAttempts.toString, e)
          if (!causesHash.contains(e.toString)) {
            causesHash = causesHash + e.toString
            causes = e :: causes
          }
          if (retriables.filter(r => r.isInstance(e)).nonEmpty) {
            if (attempt < maxAttempts) {
              Thread.sleep(attempt * delay)
              handle(e)
            }
            else {
              if (toThrow == classOf[RetriesExhaustedException]) throw RetriesExhaustedException(causes.toList)
              else throw toThrow.getConstructor(classOf[Throwable]).newInstance(e)
            }
          } else
            throw e //UnretriableException(e)
      }
    }

    throw RetriesExhaustedException(causes.toList)
  }
}

case class TryAndGiveUp() extends RunPolicy {
  override def apply[T](block: () => T): Option[T] = Retry(1, List(classOf[Throwable]), 0, RunPolicy.noop)(block)
}

case class RetryAndGiveUp(maxAttempts: Int, retriables: List[Class[_ <: Throwable]], delay: Long = 3000) extends RunPolicy {

  import Retry.logger

  override def apply[T](block: () => T): Option[T] = {
    try {
      Retry(maxAttempts, retriables, delay, RunPolicy.noop)(block)
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

//case class UnretriableException(cause: Throwable) extends Throwable

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

object SleepUninterrupted {
  def apply(ms: Long, factor: Int) {
    TryAndGiveUp()(() => {
      Thread.sleep(ms * (factor + 1))
    })
  }
}