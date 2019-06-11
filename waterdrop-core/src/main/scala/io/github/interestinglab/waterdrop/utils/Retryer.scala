package io.github.interestinglab.waterdrop.utils

/**
 * task retry executor
 * @author jiaquanyu
 *
 */
class Retryer extends Serializable {

  private var MAX_RETRY_COUNT = 10

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
          retryCount += 1

          if (retryCount == MAX_RETRY_COUNT) {
            println("[ERROR] task failed!!!")
            throw ex
          }

          Thread.sleep(5000)
          println("error: " + ex.getMessage + "\npreparing for a " + retryCount + " attempt")
      }
      wasApplied
    }
    result.get
  }
}
