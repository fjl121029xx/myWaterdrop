package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.sql.{PreparedStatement, Timestamp}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import io.github.interestinglab.waterdrop.utils.{MysqlWraper, MysqlWriter, Retryer, StructuredUtils}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Jdbc extends BaseStructuredStreamingOutput {

  var config = ConfigFactory.empty()
  var tableName: String = _
  var mysqlWraper: MysqlWraper = _
  var columns:List[String] = List.empty
  var fields:Array[String] = _

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

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
        "trigger_type" -> "default",
        "streaming_output_mode" -> "Append",
        "include_deletion" -> false
      )
    )
    config = config.withFallback(defaultConfig)
    tableName = config.getString("table")

    //get table columns
    val db = config.getString("url").reverse.split("/")(0).reverse
    columns = MysqlWriter(config.getString("url"), config.getString("username"), config.getString("password"))
      .getColWithDataType(db, tableName).map(_._1)

  }


  override def open(partitionId: Long, epochId: Long): Boolean = {
    // mysql mysql connect
    mysqlWraper = MysqlWraper(config.getString("url"), config.getString("username"), config.getString("password"))
    true
  }


  override def process(row: Row) : Unit = {

    config.getBoolean("include_deletion") match {
      case true => {
        row.getString(row.fieldIndex("actionType")) match {
          case "DELETE" => {
            val primaryKey = config.getString("primary_key_filed")
            val sql = s"DELETE FROM $tableName where $primaryKey = ?"
            val ps = mysqlWraper.getConnection.prepareStatement(sql)

            row.get(row.fieldIndex(primaryKey)) match {
              case v: Int => ps.setInt(1, v)
              case v: Long => ps.setLong(1, v)
              case v: Float => ps.setFloat(1, v)
              case v: Double => ps.setDouble(1, v)
              case v: String => ps.setString(1, v)
              case v: Timestamp => ps.setTimestamp(1, v)
            }
            ps.execute()
          }
          case _ => executeInsert(row)
        }
      }
      case false => executeInsert(row)
    }
  }

  private def executeInsert(row: Row): Unit = {

    val fieldStr = fields.mkString("(", ",", ")")

    val sb = new StringBuffer()
    for (_ <- fields.toIndexedSeq) yield sb.append("?")
    val valueStr = sb.toString.mkString("(", ",", ")")

    val sql = config.getString("jdbc_output_mode") match {
      case "replace" => s"REPLACE INTO $tableName$fieldStr VALUES$valueStr"
      case "insert ignore" => s"INSERT IGNORE INTO $tableName$fieldStr VALUES$valueStr"
      case _ => throw new RuntimeException("unknown output_mode,only support [replace] and [insert ignore]")
    }

    new Retryer().execute(() -> {
      val ps = mysqlWraper.getConnection.prepareStatement(sql)
      setPrepareStatement(fields, row, ps)
      ps.execute()
      ps.closeOnCompletion()
    })
  }

  private def setPrepareStatement(fields:Array[String],row: Row, ps: PreparedStatement): Unit = {

    var p = 1
    val indexs = fields.map(row.fieldIndex(_))
    for (i <- 0 until row.size) {
      if (indexs.contains(i)) {
        row.get(i) match {
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

  override def close(errorOrNull: Throwable): Unit = {
    mysqlWraper.close()
  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {

    val dfFill = df.na.fill("").na.fill(0L).na.fill(0).na.fill(0.0)

    fields = dfFill.schema.fieldNames.intersect(columns)

    println("[INFO] fields:" + fields.toList)

    var writer = dfFill.writeStream.outputMode(config.getString("streaming_output_mode")).foreach(this)


    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config, writer)
  }


}