package io.github.interestinglab.waterdrop.metrics

import java.io.EOFException
import java.net.SocketTimeoutException
import java.sql.{BatchUpdateException, PreparedStatement, SQLException, SQLTimeoutException, Types}
import java.util

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException
import io.github.interestinglab.waterdrop.output.Mysql
import io.github.interestinglab.waterdrop.utils._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ListBuffer

class MysqlOutputMetrics extends OutputMetrics
  with Serializable {

  private[this] var it: Iterator[Row] = _
  private[this] var cols: Array[String] = _
  private[this] var ps: PreparedStatement = _
  private[this] var retryConf: Map[String, String] = _
  private[this] var mysqlOutput: Mysql = _

  private[this] var table: String = _

  private[this] var correct_accumulator: LongAccumulator = _
  private[this] var error_accumulator: LongAccumulator = _
  private[this] var sum_accumulator: LongAccumulator = _

  def this(it: Iterator[Row], cols: Array[String], ps: PreparedStatement, retryConf: Map[String, String],
           ms: Mysql) {
    this
    this.it = it
    this.cols = cols
    this.ps = ps
    this.retryConf = retryConf
    this.mysqlOutput = ms

    this.table = mysqlOutput.config.getString("table")
    this.correct_accumulator = mysqlOutput.accumulators.get("mysql_" + table + "_correct_accu")
    this.error_accumulator = mysqlOutput.accumulators.get("mysql_" + table + "_error_accu")
    this.sum_accumulator = mysqlOutput.accumulators.get("mysql_" + table + "_sum_accu")
  }

  override def checkHasAccumulator(): Boolean = {
    correct_accumulator != null && error_accumulator != null && sum_accumulator != null
  }

  def iterProcess(): Int = {

    var i = 0
    var sum = 0
    val lb = new ListBuffer[String]
    val runningRow = new ListBuffer[Row]

    while (it.hasNext) {
      val row = it.next
      mysqlOutput.setPrepareStatement(cols, row, ps)
      lb.append(ps.toString)
      runningRow.append(row)

      ps.addBatch()
      i += 1

      if (i == mysqlOutput.config.getInt("batch.count") || (!it.hasNext)) {

        try {

          val result = ps.executeBatch()
          result.foreach(i => if (i > 0 || i == -2) correct_accumulator.add(1L) else error_accumulator.add(1L))
          sum_accumulator.add(result.length * 1L)
          sum += result.length
        } catch {
          case ex@(_: CommunicationsException | _: SocketTimeoutException | _: EOFException | _: SQLException) => {
            executeTimeException(ex, mysqlOutput.accumulators, runningRow, lb)
          }
          case e: BatchUpdateException => {
            e.printStackTrace()
          }
        }

        lb.clear
        runningRow.clear()
        ps.clearBatch()
        i = 0
      }
    }
    sum
  }

  def executeTimeException(ex: Throwable,
                           accumulators: util.HashMap[String, LongAccumulator],
                           runningRow: ListBuffer[Row],
                           lb: ListBuffer[String]
                          ): Unit = {
    println(
      s"insert table $table error ,has exception: ${ex.getMessage} ,current timestamp: ${System.currentTimeMillis()}")
    ex.printStackTrace()

    if (ex.getCause.isInstanceOf[CommunicationsException] ||
      ex.getCause.isInstanceOf[SocketTimeoutException] ||
      ex.getCause.isInstanceOf[EOFException]
    ) {
      try {
        val mr = new MysqlRetryer(retryConf, runningRow, cols)
        val result = mr.execute()
        result.foreach(i => if (i > 0 || i == -2) correct_accumulator.add(1L) else error_accumulator.add(1L))
        sum_accumulator.add(result.length * 1L)

      } catch {
        case e: Exception =>
          e.printStackTrace()
          error_accumulator.add(runningRow.length * 1L)
          sum_accumulator.add(runningRow.length * 1L)
          lb.foreach(println)
      }
    } else {
      error_accumulator.add(runningRow.length * 1L)
      sum_accumulator.add(runningRow.length * 1L)
    }
  }

}
