package io.github.interestinglab.waterdrop.output

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import java.io

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

class Hive extends BaseOutput {

  val hdfs_prefix = "hdfs://cluster"
  var conf = ConfigFactory.empty

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("database") && !conf.getString("database").trim.isEmpty ||
      conf.hasPath("table") && !conf.getString("table").trim.isEmpty ||
      conf.hasPath("path") && !conf.getString("path").trim.isEmpty match {
      case true => {
        val path = conf.getString("path")
        path.startsWith("/") match {
          case true => (true, "")
          case false => (false, "invalid path URI, it shoud be start with '/'")
        }
      }
      case false => (false, "please specify [database] [table] [path] as non-empty string")
    }
  }

  private def buildOutputPath(path: String): String = {
    val outputPath = hdfs_prefix + path + buildPatitionPath
    outputPath
  }

  private def buildPatitionPath(): String = {
    val partition = {
      if ("none".equals(conf.getString("partition"))) {
        ""
      } else {
        getPartitionStr(conf.getString("partition").split(":")(0), conf.getString("partition").split(":")(1))
      }
    }
    partition
  }

  private def getPartitionStr(pt: String, offset: String): String = {
    val pattern = pt match {
      case "hour" => "yyyyMMddHH"
      case "day" => "yyyyMMdd"
    }
    val pt_str = "/" + new SimpleDateFormat(pattern).format(new Date())
    pt_str
  }

  private def getColCompatMap(cols: List[String], arr: Array[String]): mutable.HashMap[String, Int] = {
    var map = new mutable.HashMap[String, Int]
    arr.foreach(key =>
      if (cols.contains(key.toLowerCase)) {
        map += key.toLowerCase -> arr.indexOf(key)
    })
    map
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "partition" -> "none",
        "save_mode" -> "error",
        "serializer" -> "text",
        "delimiter" -> "\u0001"
      )
    )

    conf = conf.withFallback(defaultConfig)

  }

  override def process(df: Dataset[Row]): Unit = {

    import df.sparkSession.implicits._

    val cols = getColName(df.sparkSession)
    val out_path = buildOutputPath(conf.getString("path"))
    val delim = conf.getString("delimiter")

    val compatMap = getColCompatMap(cols, df.columns)

    val out_ds = df.map(row => {
      val sb = new StringBuilder()

      cols.foreach(col => {
        if (compatMap.contains(col)) {
          val col_index = compatMap.get(col)
          sb.append(row.get(col_index.get))
        } else {
          sb.append("\\N")
        }
        sb.append(delim)
      })
      sb.substring(0, sb.length - 1)
    })

    val writer = out_ds.write.mode(conf.getString("save_mode"))
    writer.text(out_path)

  }

  private def getColName(sparkSession: SparkSession): List[String] = {

    val db = conf.getString("database")
    val table = conf.getString("table")

    val colNameArr = sparkSession
      .sql("desc " + db + "." + table)
      .toDF()
      .select("col_name")
      .collect()

    colNameArr.map(_.get(0).toString).toList
  }
}
