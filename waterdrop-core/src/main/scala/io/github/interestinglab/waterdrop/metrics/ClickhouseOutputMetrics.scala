package io.github.interestinglab.waterdrop.metrics

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.output.Clickhouse
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl, ClickHousePreparedStatement}

import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

class ClickhouseOutputMetrics extends OutputMetrics
  with Serializable {

  var accumulators: HashMap[String, LongAccumulator] = _
  var schema: Map[String, String] = _
  var jdbcLink: String = _
  var initSQL: String = _
  var clickhouseOutput: Clickhouse = _

  var table: String = _

  var correct_accumulator: LongAccumulator = _
  var error_accumulator: LongAccumulator = _
  var sum_accumulator: LongAccumulator = _

  def this(accumulators: HashMap[String, LongAccumulator],
           schema: Map[String, String],
           jdbcLink: String,
           initSQL: String,
           ch: Clickhouse) {
    this()
    this.accumulators = accumulators
    this.schema = schema
    this.jdbcLink = jdbcLink
    this.initSQL = initSQL
    this.clickhouseOutput = ch

    this.table = clickhouseOutput.config.getString("table")
    this.correct_accumulator = clickhouseOutput.accumulators.get("clickhouse_" + table + "_correct_accu")
    this.error_accumulator = clickhouseOutput.accumulators.get("clickhouse_" + table + "_error_accu")
    this.sum_accumulator = clickhouseOutput.accumulators.get("clickhouse_" + table + "_sum_accu")
  }


  def process(df: Dataset[Row]): Unit = {
    val bulkSize = clickhouseOutput.config.getInt("bulk_size")
    df.foreachPartition {
      iter =>
        val executorBalanced = new BalancedClickhouseDataSource(jdbcLink)
        val executorConn = clickhouseOutput.config.hasPath("username") match {
          case true =>
            executorBalanced
              .getConnection(clickhouseOutput.config.getString("username"), clickhouseOutput.config.getString("password"))
              .asInstanceOf[ClickHouseConnectionImpl]
          case false => executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
        }
        val statement = executorConn.createClickHousePreparedStatement(initSQL)
        var length = 0
        while (iter.hasNext) {
          val item = iter.next()
          length += 1
          clickhouseOutput.renderStatement(clickhouseOutput.fields, item, statement)
          statement.addBatch()

          if (length >= bulkSize) {
            try {
              val result = statement.executeBatch()
              result.foreach(i => if (i > 0 || i == -2) correct_accumulator.add(1L) else error_accumulator.add(1L))
              sum_accumulator.add(result.length * 1L)
            } catch {
              case ex: Exception =>
                error_accumulator.add(length * 1L)
                sum_accumulator.add(length * 1L)
            }

            length = 0
          }
        }

        statement.executeBatch()
    }
  }


}
