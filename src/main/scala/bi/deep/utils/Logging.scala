package bi.deep.utils

import org.apache.log4j.Logger


trait Logging {
  @transient protected lazy val logger: Logger = Logger.getLogger(getClass.getName)

  protected def logInfo(msg: => String): Unit = {
    if (logger.isInfoEnabled) logger.info(msg)
  }

  protected def logDebug(msg: => String): Unit = {
    if (logger.isDebugEnabled) logger.debug(msg)
  }

  protected def logWarn(msg: => String): Unit = {
    logger.warn(msg)
  }

  protected def logError(msg: => String, throwable: Throwable = null): Unit = {
    if (throwable == null) logger.error(msg)
    else logger.error(msg, throwable)
  }
}

