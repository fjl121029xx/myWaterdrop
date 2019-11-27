package io.github.interestinglab.waterdrop.output

import java.io.EOFException
import java.net.SocketTimeoutException
import java.sql.{BatchUpdateException, DriverManager, PreparedStatement, SQLException, SQLTimeoutException, Types}
import java.util
import java.util.concurrent.CompletableFuture

import com.alibaba.fastjson.JSON
import com.mysql.jdbc.exceptions.MySQLNonTransientConnectionException
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.filter.{Convert, Recent, Schema, Sql}
import io.github.interestinglab.waterdrop.metrics.MysqlOutputMetrics
import io.github.interestinglab.waterdrop.utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.util.control._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Mysql extends BaseOutput {

  var config: Config = ConfigFactory.empty()
  var table: String = _

  var columnWithDataTypes: List[(String, String)] = _
  var columnWithDefaultValue: Map[String, Object] = _

  var columns: List[String] = List.empty
  var fields: Array[String] = _
  var defaultFiledValue: Map[String, String] = _

  val retryer = new Retryer

  var filterSchema: Schema = _
  var filterRecent: Recent = _
  var filterConvert: Convert = _
  var filterSql: Sql = _

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("url") && config.hasPath("username") &&
      config.hasPath("table") && config.hasPath("password") match {
      case true => {
        config.hasPath("include_deletion") match {
          case true => if (config.hasPath("primary_key_filed")) (true, "") else (false, "please specify [primary_key_filed]!!!")
          case false => (true, "")
        }
      }
      case false => (false, "please specify [url] and [username] and [table] and [password]!!!")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "driver" -> "com.mysql.jdbc.driver", // allowed values: overwrite, append, ignore, error
        "jdbc_output_mode" -> "replace",
        "include_deletion" -> false,
        "batch.count" -> 100, // insert batch count
        "insert.mode" -> "REPLACE", // INSERT IGNORE or REPLACE
        "table_filter" -> false
        //table_filter_regex -> ""
        //"table_recent" -> "",
        //"table_convert" -> "",
        //"table_sql" -> ""
      )
    )

    config = config.withFallback(defaultConfig)
    table = config.getString("table")

    val db = config.getString("url")
      .reverse.split('?')(if (config.getString("url").contains("?")) 1 else 0)
      .split("/")(0).reverse

    val mysqlWriter = MysqlWriter(config.getString("url"), config.getString("username"), config.getString("password"))

    //get table columns
    columnWithDataTypes = mysqlWriter.getColWithDataType(db, table)
    columns = columnWithDataTypes.map(_._1)

    if (config.getBoolean("table_filter")) {
      filterSchema = new Schema {
        {
          setConfig(ConfigFactory.parseMap(
            Map(
              "schema" -> SchemaUtils.getSchemaString(columnWithDataTypes),
              "source" -> "fields")))
        }
      }
      filterSchema.prepare(spark)
    }

    if (config.hasPath("table_recent")) {
      filterRecent = new Recent {
        {
          setConfig(ConfigFactory.parseMap(Map("union.fields" -> config.getString("table_recent"))))
        }
      }
    }

    if (config.hasPath("table_convert")) {
      filterConvert = new Convert {
        {
          setConfig(ConfigFactory.parseMap(JSON.parseObject(config.getString("table_convert"))))
        }
      }
      filterConvert.prepare(spark)
    }

    if (config.hasPath("table_sql")) {
      filterSql = new Sql {
        {
          setConfig(ConfigFactory.parseMap(JSON.parseObject(config.getString("table_sql"))))
        }
      }
      filterSql.prepare(spark)
    }
  }

  override def process(df: Dataset[Row]): Unit = {

    var tmpdf = tableFilter(df)
    tmpdf = tableConvert(tmpdf)
    tmpdf = tableRecent(tmpdf)
    tmpdf = tableSql(tmpdf)

    var dfFill = tmpdf

    val urlBroad = df.sparkSession.sparkContext.broadcast(config.getString("url"))
    val userBroad = df.sparkSession.sparkContext.broadcast(config.getString("username"))
    val passwdBroad = df.sparkSession.sparkContext.broadcast(config.getString("password"))

    if (config.getBoolean("include_deletion")) {
      dfFill.cache //获取删除数据
      val primaryKey = config.getString("primary_key_filed")
      val delSql = s"DELETE FROM $table where $primaryKey = ?"

      val primaryKeyBroad = df.sparkSession.sparkContext.broadcast(primaryKey)
      val delSqlBroad = df.sparkSession.sparkContext.broadcast(delSql)

      dfFill.where("actionType=\"DELETE\"").foreachPartition(it => {

        val conn = DriverManager.getConnection(urlBroad.value, MysqlWraper.getJdbcConf(userBroad.value, passwdBroad.value))
        val ps = conn.prepareStatement(delSqlBroad.value)
        val map = Map(
          "url" -> urlBroad.value,
          "user" -> userBroad.value,
          "passwd" -> passwdBroad.value,
          "sql" -> delSqlBroad.value
        )
        iterProcess(it, Array(primaryKeyBroad.value), ps, map)

        retryer.execute(ps.close())
        retryer.execute(conn.close())
      })
      dfFill = dfFill.where("actionType!=\"DELETE\"")
    }

    fields = tmpdf.schema.fieldNames.intersect(columns)
    val fieldStr = fields.mkString("(", ",", ")")

    val sb = new StringBuffer()
    for (_ <- fields.toIndexedSeq) yield sb.append("?")
    val valueStr = sb.toString.mkString("(", ",", ")")

    val sql = config.getString("jdbc_output_mode") match {
      case "replace" => s"REPLACE INTO $table$fieldStr VALUES$valueStr"
      case "insert ignore" => s"INSERT IGNORE INTO $table$fieldStr VALUES$valueStr"
      case _ => throw new RuntimeException("unknown output_mode,only support [replace] and [insert ignore]")
    }

    val insertAcc = df.sparkSession.sparkContext.longAccumulator
    val sqlBroad = df.sparkSession.sparkContext.broadcast(sql)

    val startTime = System.currentTimeMillis

    dfFill.foreachPartition(it => {
      val conn = MysqlRetryer.getConnByRetryer(urlBroad.value, userBroad.value, passwdBroad.value).get
      //      val conn = DriverManager.getConnection(urlBroad.value, MysqlWraper.getJdbcConf(userBroad.value, passwdBroad.value))
      val ps = conn.prepareStatement(sqlBroad.value)

      val retryMap = Map(
        "url" -> urlBroad.value,
        "user" -> userBroad.value,
        "passwd" -> passwdBroad.value,
        "sql" -> sqlBroad.value
      )

      //            insertAcc.add(iterProcess(it, fields, ps, retryMap))

      val mysqlMetrics = new MysqlOutputMetrics(it, fields, ps, retryMap, ms = this)
      insertAcc.add(mysqlMetrics.iterProcess())

      retryer.execute(ps.close())
      retryer.execute(conn.close())
    })

    dfFill.unpersist

    println(s"[INFO]insert table ${config.getString("table")} count: ${insertAcc.value} , time consuming: ${System.currentTimeMillis - startTime}")

    insertAcc.reset
  }

  /**
   * 1）根据表名过滤数据
   * 2) schema转换
   */
  private def tableFilter(df: Dataset[Row]): Dataset[Row] = {

    config.getBoolean("table_filter") match {
      case true => {
        val condition = config.hasPath("table_filter_regex") match {
          case true => col("tableName").rlike(config.getString("table_filter_regex"))
          case false => lower(col("tableName")).startsWith(config.getString("table").toLowerCase)
        }
        filterSchema.process(df.filter(condition))
      }
      case false => df
    }
  }

  private def tableConvert(df: Dataset[Row]): Dataset[Row] = {

    config.hasPath("table_convert") match {
      case true => filterConvert.process(df)
      case false => df
    }
  }

  private def tableRecent(df: Dataset[Row]): Dataset[Row] = {

    config.hasPath("table_recent") match {
      case true => filterRecent.process(df)
      case false => df
    }
  }


  private def tableSql(df: Dataset[Row]): Dataset[Row] = {

    config.hasPath("table_sql") match {
      case true => filterSql.process(df.sparkSession, df)
      case false => df
    }
  }

  /**
   * mysql 超时重试
   */
  private def iterProcess(it: Iterator[Row], cols: Array[String], ps: PreparedStatement, retryConf: Map[String, String]): Int = {

    var i = 0
    var sum = 0
    val lb = new ListBuffer[String]
    var runningRow = new ListBuffer[Row]

    while (it.hasNext) {
      val row = it.next
      setPrepareStatement(cols, row, ps)
      runningRow.append(row)
      lb.append(ps.toString)
      ps.addBatch()
      i += 1
      if (i == config.getInt("batch.count") || (!it.hasNext)) {
        try {
          sum += ps.executeBatch.length
        } catch {
          case ex@(_: CommunicationsException | _: SocketTimeoutException | _: EOFException | _: SQLException) => {
            println(
              s"insert table $table error ,has exception: ${ex.getMessage} ,current timestamp: ${System.currentTimeMillis()}")
            ex.printStackTrace()

            if (ex.getCause.isInstanceOf[CommunicationsException] ||
              ex.getCause.isInstanceOf[SocketTimeoutException] ||
              ex.getCause.isInstanceOf[EOFException]
            ) {
              try {
                val mr = new MysqlRetryer(retryConf, runningRow, cols)
                sum += mr.execute().length
              } catch {
                case e: Exception =>
                  e.printStackTrace()
                  lb.foreach(println)
              }
            }
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
    runningRow = null
    sum
  }


  def setPrepareStatement(fields: Array[String], row: Row, ps: PreparedStatement): Unit = {

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
