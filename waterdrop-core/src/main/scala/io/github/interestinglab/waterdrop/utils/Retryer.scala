package io.github.interestinglab.waterdrop.utils

import java.sql.Connection

/**
 * task retry executor
 *
 * @author jiaquanyu
 *
 */
class Retryer extends Serializable {

   var MAX_RETRY_COUNT = 10

  def this(maxRetryCount: Int) {
    this()
    MAX_RETRY_COUNT = maxRetryCount
  }

  def execute(anyFail: => Any): Any = {

    var retryCount = 1
    var wasApplied = false
    var result = None: Option[Any]

    while (!wasApplied && retryCount <= MAX_RETRY_COUNT) {
      try {
          result = Some(anyFail)
          wasApplied = true
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          retryCount += 1
          ex.printStackTrace()
          if (retryCount == MAX_RETRY_COUNT) {
            println("[ERROR] task failed!!!")
            throw ex
          }


          println("error: " + ex.getMessage + "\npreparing for a " + retryCount + " attempt")
      }
      wasApplied
    }
    result.get
  }
}
