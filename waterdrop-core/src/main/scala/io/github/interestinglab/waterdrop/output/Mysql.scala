package io.github.interestinglab.waterdrop.output

import java.sql.{PreparedStatement, Timestamp}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.filter.{Recent, Schema}
import io.github.interestinglab.waterdrop.utils.{MysqlWraper, MysqlWriter, Retryer, SchemaUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Mysql extends BaseOutput {

  var config: Config = ConfigFactory.empty()
  var table: String = _

  var columnWithDataTypes: List[(String, String)] = _
  var columns: List[String] = List.empty
  var fields: Array[String] = _

  val retryer = new Retryer
  var mysqlWraper: Broadcast[MysqlWraper] = _

  var filterSchema: Schema = _
  var filterRecent: Recent = _

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("url") && config.hasPath("username") && config.hasPath("table")
    config.hasPath("password") match {
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
        "table_filter" -> false,
        "table_recent_fields" -> ""
      )
    )

    mysqlWraper = spark.sparkContext.broadcast(
      MysqlWraper(config.getString("url"), config.getString("username"), config.getString("password")))

    config = config.withFallback(defaultConfig)
    table = config.getString("table")

    //get table columns
    val db = config.getString("url")
      .reverse.split('?')(if (config.getString("url").contains("?")) 1 else 0)
      .split("/")(0).reverse

    columnWithDataTypes = MysqlWriter(config.getString("url"), config.getString("username"), config.getString("password")).getColWithDataType(db, table)

    columns = columnWithDataTypes.map(_._1)

    if (config.getBoolean("table_filter")) {
      filterSchema = new Schema {{
          setConfig(ConfigFactory.parseMap(
            Map(
              "schema" -> SchemaUtils.getSchemaString(columnWithDataTypes),
               "source"->"fields")))
        }}
      filterSchema.prepare(spark)
    }

    if (config.getString("table_recent_fields").length > 0){
      filterRecent = new Recent {{
          setConfig(ConfigFactory.parseMap(Map("union.fields" -> config.getString("table_recent_fields"))))
        }}
    }
  }

  override def process(df: Dataset[Row]): Unit = {

    var tmpdf = tableFilter(df)
    tmpdf = tableRecent(tmpdf)

    var dfFill = tmpdf.na.fill("").na.fill(0L).na.fill(0).na.fill(0.0)

    if (config.getBoolean("include_deletion")) {
      dfFill.cache //获取删除数据
      val primaryKey = config.getString("primary_key_filed")
      val delSql = s"DELETE FROM $table where $primaryKey = ?"

      val primaryKeyBroad = df.sparkSession.sparkContext.broadcast(primaryKey)
      val delSqlBroad = df.sparkSession.sparkContext.broadcast(delSql)

      dfFill.where("actionType=\"DELETE\"").foreachPartition(it => {
        val ps = retryer.execute(mysqlWraper.value.getConnection.prepareStatement(delSqlBroad.value)).asInstanceOf[PreparedStatement]
        iterProcess(it, Array(primaryKeyBroad.value), ps)
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

    val startTime = System.currentTimeMillis()

    val insertAcc = df.sparkSession.sparkContext.longAccumulator
    val sqlBroad = df.sparkSession.sparkContext.broadcast(sql)

    dfFill.foreachPartition(it => {
      val ps = retryer.execute(mysqlWraper.value.getConnection.prepareStatement(sqlBroad.value)).asInstanceOf[PreparedStatement]
      insertAcc.add(iterProcess(it,fields,ps))
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
      case true => filterSchema.process(df.filter(col("tableName").startsWith(config.getString("table"))))
      case false => df
    }
  }

  private def tableRecent(df: Dataset[Row]): Dataset[Row] = {

    config.getString("table_recent_fields") match {
      case "" => df
      case _ => filterRecent.process(df)
    }
  }

  private def iterProcess(it: Iterator[Row], cols: Array[String], ps: PreparedStatement): Int = {

    var i = 0
    var sum = 0

    while (it.hasNext) {
      val row = it.next
      setPrepareStatement(cols, row, ps)
      ps.addBatch()
      i += 1

      if (i == config.getInt("batch.count") || (!it.hasNext)) {
        val j = retryer.execute(ps.executeBatch).asInstanceOf[Array[Int]]
        sum += j.length
        i = 0
      }
    }
    sum
  }

  private def setPrepareStatement(fields: Array[String], row: Row, ps: PreparedStatement): Unit = {

    var p = 1
    val indexs = fields.map(row.fieldIndex(_))
    for (i <- 0 until row.size) {
      if (indexs.contains(i)) {
        row.get(i) match {
          case v: Short => ps.setInt(p, v)
          case v: Int => ps.setInt(p, v)
          case v: Long => ps.setLong(p, v)
          case v: Float => ps.setFloat(p, v)
          case v: Double => ps.setDouble(p, v)
          case v: String => ps.setString(p, v)
          case v: Timestamp => ps.setTimestamp(p, v)
        }
        p += 1
      }
    }
  }

}
