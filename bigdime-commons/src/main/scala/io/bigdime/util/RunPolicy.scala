package io.bigdime.util

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

case class Retry(maxAttempts: Int, retriables: List[Class[_ <: Throwable]], delay: Long = 3000) extends RunPolicy {
  override def apply[T](block: () => T): Option[T] = {
    var attempt = 0

    var causes = new ListBuffer[Throwable]
    while (true) {
      attempt += 1
      try {
        return Some(block())
      } catch {
        case e: Exception =>
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
  override def apply[T](block: () => T): Option[T] = {
    try {
      Retry(maxAttempts, retriables, delay)(block)
    } catch {
      case e: Exception => None
    }
  }
}

case class RetriesExhaustedException(causes: List[Throwable]) extends Throwable
