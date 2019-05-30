package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.sql.{PreparedStatement, Timestamp}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import io.github.interestinglab.waterdrop.utils.{MysqlWriter, StructuredUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Jdbc extends BaseStructuredStreamingOutput{

  var config = ConfigFactory.empty()

  var tableName: String = _
  var mysqlWriter:Broadcast[MysqlWriter] = _

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig(): Config = config

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("url") && config.hasPath("username") && config.hasPath("table")
    config.hasPath("password") match {
      case true => (true,"")
      case false => (false,"please specify [url] and [username] and [table] and [password]")
    }

  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)


    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "driver" -> "com.mysql.jdbc.driver", // allowed values: overwrite, append, ignore, error
        "jdbc_output_mode" -> "replace",
        "trigger_type" -> "default",
        "streaming_output_mode" -> "Append"
      )
    )
    config = config.withFallback(defaultConfig)
    tableName = config.getString("table")

    // mysql mysql connect
    mysqlWriter = spark.sparkContext
      .broadcast(MysqlWriter(config.getString("url"), config.getString("username"), config.getString("password")))
  }


  override def open(partitionId: Long, epochId: Long): Boolean = true


  override def process(row: Row): Unit = {

    val fields = row.schema.fieldNames
    val fieldStr = fields.mkString("(",",",")")

    val sb = new StringBuffer()
    for (_ <- 0 until fields.length) yield sb.append("?")
    val valueStr = sb.toString.mkString("(",",",")")

    val sql = config.getString("jdbc_output_mode") match {
      case "replace" => s"REPLACE INTO $tableName$fieldStr VALUES$valueStr"
      case "insert ignore" => s"INSERT IGNORE INTO $tableName$fieldStr VALUES$valueStr"
      case _ => throw new RuntimeException("unknown output_mode,only support [replace] and [insert ignore]")
    }

    val ps = mysqlWriter.value.getConnection().prepareStatement(sql)
    setPrepareStatement(row,ps)
    ps.execute()
  }

  private def setPrepareStatement(row: Row,ps: PreparedStatement): Unit = {

    for (i <- 0 until row.size) {
      row.get(i) match {
        case v: Int => ps.setInt(i + 1, v)
        case v: Long => ps.setLong(i + 1, v)
        case v: Float => ps.setFloat(i + 1, v)
        case v: Double => ps.setDouble(i + 1, v)
        case v: String => ps.setString(i + 1, v)
        case v: Timestamp => ps.setTimestamp(i + 1, v)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {

    var writer = df.writeStream
      .outputMode(config.getString("streaming_output_mode"))
      .foreach(this)

    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config, writer)
  }


}