package io.github.interestinglab.waterdrop.utils

import java.sql.{DriverManager, PreparedStatement, SQLException, SQLTimeoutException, Types}
import java.util.concurrent.CompletableFuture

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class MysqlRetryer(mysqlmap: Map[String, String],
                   runningRow: ListBuffer[Row],
                   cols: Array[String]) extends Retryer {

  def execute(): Array[Int] = {
    var retryCount = 1
    var wasApplied = false
    var result = None: Option[Array[Int]]

    while (!wasApplied && retryCount <= MAX_RETRY_COUNT) {
      try {
        val conn = DriverManager.getConnection(mysqlmap("url"), MysqlWraper.getJdbcConf(mysqlmap("user"), mysqlmap("passwd")))

        val ps = conn.prepareStatement(mysqlmap("sql"))

        val itsRow = runningRow.iterator
        while (itsRow.hasNext) {
          val r = itsRow.next()
          setPrepareStatement(cols, r, ps)
          ps.addBatch()
        }
        conn.setAutoCommit(false)
        val sp = conn.setSavepoint("mysql_retruer_" + retryCount + "_sp")
        try {
          result = Some(ps.executeBatch())
          conn.commit()
          wasApplied = true
        } catch {
          case e: Exception =>
            e.printStackTrace()
            try {
              conn.rollback(sp)
            } catch {
              case e: Exception =>
                e.printStackTrace()
            }

            retryCount += 1
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }

    }
    result.get
  }


  private def setPrepareStatement(fields: Array[String], row: Row, ps: PreparedStatement): Unit = {

    var p = 1
    val indexs = fields.map(row.fieldIndex(_))
    for (i <- 0 until row.size) {
      if (indexs.contains(i)) {
        row.schema.get(i).dataType match {
          case ShortType => if (row.get(i) != null) ps.setShort(p, row.getShort(i)) else ps.setNull(p, Types.INTEGER)
          case IntegerType => if (row.get(i) != null) ps.setInt(p, row.getInt(i)) else ps.setNull(p, Types.INTEGER)
          case LongType => if (row.get(i) != null) ps.setLong(p, row.getLong(i)) else ps.setNull(p, Types.BIGINT)
          case FloatType => if (row.get(i) != null) ps.setFloat(p, row.getFloat(i)) else ps.setNull(p, Types.FLOAT)
          case DoubleType => if (row.get(i) != null) ps.setDouble(p, row.getDouble(i)) else ps.setNull(p, Types.DOUBLE)
          case StringType => ps.setString(p, row.getString(i))
          case TimestampType => ps.setTimestamp(p, row.getTimestamp(i))
          case t: DecimalType => ps.setBigDecimal(p, row.getDecimal(i))
        }
        p += 1
      }
    }
  }
}
