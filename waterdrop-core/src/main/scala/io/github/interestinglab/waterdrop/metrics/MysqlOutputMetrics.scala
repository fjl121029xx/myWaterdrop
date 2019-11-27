package io.github.interestinglab.waterdrop.metrics

import java.io.EOFException
import java.net.SocketTimeoutException
import java.sql.{BatchUpdateException, DriverManager, PreparedStatement, SQLException, SQLTimeoutException, Types}
import java.util

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.utils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


class MysqlOutputMetrics(config: Config, ps: PreparedStatement, it: Iterator[Row],
                         cols: Array[String],
                         accumulators: util.HashMap[String, LongAccumulator],
                         mysqlmap: Map[String, String]) {
  val table: String = config.getString("table")

  val correct_accumulator: LongAccumulator = accumulators.get("mysql_" + table + "_correct_accu")
  val error_accumulator: LongAccumulator = accumulators.get("mysql_" + table + "_error_accu")
  val sum_accumulator: LongAccumulator = accumulators.get("mysql_" + table + "_sum_accu")

  def iterProcess(): Int = {

    var i = 0
    var sum = 0
    val lb = new ListBuffer[String]
    val runningRow = new ListBuffer[Row]

    while (it.hasNext) {
      val row = it.next
      setPrepareStatement(cols, row, ps)
      lb.append(ps.toString)
      runningRow.append(row)

      ps.addBatch()
      i += 1

      if (i == config.getInt("batch.count") || (!it.hasNext)) {

        try {
          val result = ps.executeBatch()
          result.foreach(i => if (i > 0 || i == -2) correct_accumulator.add(1L) else error_accumulator.add(1L))
          sum_accumulator.add(result.length * 1L)
          sum += result.length
        } catch {
          case ex@(_: CommunicationsException | _: SocketTimeoutException | _: EOFException | _: SQLException) => {
            executeTimeException(ex, accumulators, runningRow, lb)
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

    val table = config.getString("table")

    val correct_accumulator = accumulators.get("mysql_" + table + "_correct_accu")
    val error_accumulator = accumulators.get("mysql_" + table + "_error_accu")
    val sum_accumulator = accumulators.get("mysql_" + table + "_sum_accu")

    println(
      s"insert table $table error ,has exception: ${ex.getMessage} ,current timestamp: ${System.currentTimeMillis()}")
    ex.printStackTrace()

    if (ex.getCause.isInstanceOf[CommunicationsException] ||
      ex.getCause.isInstanceOf[SocketTimeoutException] ||
      ex.getCause.isInstanceOf[EOFException]
    ) {
      try {
        val mr = new MysqlRetryer(mysqlmap, runningRow, cols)
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

  //  override def processWithMetrics(df: Dataset[Row], accu_map: util.HashMap[String, LongAccumulator]): Unit = {
  //    super.processWithMetrics(df, accu_map)
  //    var tmpdf = tableFilter(df)
  //    tmpdf = tableConvert(tmpdf)
  //    tmpdf = tableRecent(tmpdf)
  //    tmpdf = tableSql(tmpdf)
  //    var dfFill = tmpdf
  //
  //    val sparkSession = df.sparkSession
  //    val urlBroad = sparkSession.sparkContext.broadcast(config.getString("url"))
  //    val userBroad = sparkSession.sparkContext.broadcast(config.getString("username"))
  //    val passwdBroad = sparkSession.sparkContext.broadcast(config.getString("password"))
  //
  //    if (config.getBoolean("include_deletion")) {
  //      dfFill.cache //获取删除数据
  //      val primaryKey = config.getString("primary_key_filed")
  //      val delSql = s"DELETE FROM $table where $primaryKey = ?"
  //
  //      val primaryKeyBroad = df.sparkSession.sparkContext.broadcast(primaryKey)
  //      val delSqlBroad = df.sparkSession.sparkContext.broadcast(delSql)
  //
  //      dfFill.where("actionType=\"DELETE\"").foreachPartition(it => {
  //
  //        val conn = DriverManager.getConnection(urlBroad.value, MysqlWraper.getJdbcConf(userBroad.value, passwdBroad.value))
  //        val ps = conn.prepareStatement(delSqlBroad.value)
  //
  //        iterProcess(it, Array(primaryKeyBroad.value), ps)
  //
  //        retryer.execute(ps.close())
  //        retryer.execute(conn.close())
  //      })
  //      dfFill = dfFill.where("actionType!=\"DELETE\"")
  //    }
  //
  //    fields = tmpdf.schema.fieldNames.intersect(columns)
  //    val fieldStr = fields.mkString("(", ",", ")")
  //
  //    val sb = new StringBuffer()
  //    for (_ <- fields.toIndexedSeq) yield sb.append("?")
  //    val valueStr = sb.toString.mkString("(", ",", ")")
  //
  //    val sql = config.getString("jdbc_output_mode") match {
  //      case "replace" => s"REPLACE INTO $table$fieldStr VALUES$valueStr"
  //      case "insert ignore" => s"INSERT IGNORE INTO $table$fieldStr VALUES$valueStr"
  //      case _ => throw new RuntimeException("unknown output_mode,only support [replace] and [insert ignore]")
  //    }
  //
  //    val insertAcc = df.sparkSession.sparkContext.longAccumulator
  //    val sqlBroad = df.sparkSession.sparkContext.broadcast(sql)
  //
  //    val startTime = System.currentTimeMillis
  //    //    dfFill.show()
  //    //    if(dfFill.schema.fieldNames)
  //
  //    dfFill.foreachPartition(it => {
  //      val conn = MysqlRetryer.getConnByRetryer(urlBroad.value, userBroad.value, passwdBroad.value).get
  //      //      val conn = DriverManager.getConnection(urlBroad.value, MysqlWraper.getJdbcConf(userBroad.value, passwdBroad.value))
  //      val ps = conn.prepareStatement(sqlBroad.value)
  //      try {
  //
  //        val map = Map(
  //          "url" -> urlBroad.value,
  //          "user" -> userBroad.value,
  //          "passwd" -> passwdBroad.value,
  //          "sql" -> sqlBroad.value
  //        )
  //        insertAcc.add(iterProcessWithMetrics(it, fields, ps, accu_map,
  //          map))
  //      } catch {
  //        case exe: Exception =>
  //          exe.printStackTrace()
  //      }
  //      retryer.execute(ps.close())
  //      retryer.execute(conn.close())
  //    })
  //    dfFill.unpersist
  //
  //    println(s"[INFO]insert table ${config.getString("table")} count: ${insertAcc.value} , time consuming: ${System.currentTimeMillis - startTime}")
  //
  //    insertAcc.reset
  //  }
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