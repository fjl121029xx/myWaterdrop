package io.github.interestinglab.waterdrop.utils

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, SQLTimeoutException, Types}
import java.util.concurrent.CompletableFuture

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object MysqlRetryer {

  def getConnByRetryer(url: String, user: String, passwd: String): Option[Connection] = {
    var current: Long = System.currentTimeMillis()

    var retryCount = 1
    var wasApplied = false
    var result = None: Option[Connection]
    while (!wasApplied && retryCount <= 10) {
      try {
        println("↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓")
        println(current + " connect mysql: 尝试建立mysql连接 ...ing,  第" + retryCount + "次 ")
        result = Some(DriverManager.getConnection(url, MysqlWraper.getJdbcConf(user, passwd)))
        println(current + " connect mysql: 尝试建立mysql连接 ...ing,  第" + retryCount + "次 成功 ")
        println("↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑")
        wasApplied = true
      } catch {
        case es: Exception =>
          es.printStackTrace()
          println(current + " connect mysql: 尝试建立mysql连接 ...ing,  第" + retryCount + "次 失败 ")
          println("↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑")
          retryCount += 1
          Thread.sleep(5000)
      }
    }
    println("!!! getConnByRetryer")
    Thread.sleep(10000)
    result
  }
}

class MysqlRetryer(mysqlmap: Map[String, String],
                   runningRow: ListBuffer[Row],
                   cols: Array[String]) extends Retryer {

  def execute(): Array[Int] = {
    val current: Long = System.currentTimeMillis()

    var retryCount = 1
    var wasApplied = false
    var result = None: Option[Array[Int]]
    val lb = new ListBuffer[String]
    println(current + " 开始超时重试执行 ...ing")
    while (!wasApplied && retryCount <= MAX_RETRY_COUNT) {
      try {

        println("↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓")
        println(current + " execute sql: 重试执行 ...ing,  第" + retryCount + "次 ")
        val conn = DriverManager.getConnection(mysqlmap("url"), MysqlWraper.getJdbcConf(mysqlmap("user"), mysqlmap("passwd")))
        val ps = conn.prepareStatement(mysqlmap("sql"))

        val itsRow = runningRow.iterator
        while (itsRow.hasNext) {
          val r = itsRow.next()
          setPrepareStatement(cols, r, ps)
          ps.addBatch()
          lb.append(ps.toString)
        }

        println("!!! ps.executeBatch() start")
        Thread.sleep(10000)
        result = Some(ps.executeBatch())
        println("!!! ps.executeBatch() ok")
        Thread.sleep(10000)

        wasApplied = true
        println(current + " execute sql: 重试执行 ...ing,  第" + retryCount + "次 成功")
        println("↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑")
      } catch {
        case e: Exception =>
          e.printStackTrace()
          println(current + " execute sql: 重试执行 ...ing,  第" + retryCount + "次 失败")
          println("↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑")
          retryCount += 1
          if (retryCount > MAX_RETRY_COUNT) {
            lb.foreach(println)
          }
          Thread.sleep(5000)
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
