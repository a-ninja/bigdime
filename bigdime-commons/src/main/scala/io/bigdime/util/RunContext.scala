package io.bigdime.util

import java.io.IOException


/**
  * Created by neejain on 12/22/16.
  */
object RunContext {

  def autoCloseable[E <: AutoCloseable, T](closeable: E)(block: (E) => T): T = {
    try {
      block(closeable)
    } finally {
      try
        closeable.close()
      catch {
        case e: IOException => {
          //          logger.warn(getHandlerPhase, "exception while trying to close the ByteArrayOutputStream", e)
          // duck
        }
      }
    }
  }

}
