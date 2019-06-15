package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

class Hive extends BaseOutput {

  val hdfs_prefix: String = "hdfs://"
  var conf: Config = ConfigFactory.empty

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
          case false => (false, "invalid path URI, it should be start with '/'")
        }
      }
      case false => (false, "please specify [database] [table] [path] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "error",
        "serializer" -> "text",
        "delimiter" -> "\u0001",
        "table_regex" -> ".*",
        "compression" -> "gzip"
      )
    )

    conf = conf.withFallback(defaultConfig)

  }

  override def process(df: Dataset[Row]): Unit = {

    import df.sparkSession.implicits._

    //filter empty dataset
    val cols = getColNames(df.sparkSession)
    val delim = conf.getString("delimiter")

    // match binlog column names
    val matchMap = getColMatchMap(cols, df.columns)

    // convert json to row text with delimiter
    val out_ds = df
      .filter($"mTableName".rlike(conf.getString("table_regex")))
      .map(row => {
        val sb = new StringBuilder()

        cols.foreach(col => {
          if (matchMap.contains(col)) {
            val col_index = matchMap.get(col)
            // get column value string and replace '\u0001', '\n' to space
            val colStr = row.get(col_index.get) match {
              case null => "\\N"
              case _ =>
                row
                  .get(col_index.get)
                  .toString
                  .replaceAll("\u0001", " ")
                  .replaceAll("\n", " ")
                  .replaceAll("\r", " ")
                  .replaceAll("\r\n", " ")
            }
            sb.append(colStr)
          } else {
            sb.append("\\N")
          }
          sb.append(delim)
        })
        sb.substring(0, sb.length - 1)
      })

    out_ds.write
      .mode(conf.getString("save_mode"))
      .option("compression", conf.getString("compression"))
      .text(conf.getString("path"))

  }

  private def getColMatchMap(cols: List[String], arr: Array[String]): mutable.HashMap[String, Int] = {
    var map = new mutable.HashMap[String, Int]
    arr.foreach(key =>
      if (cols.contains(key.toLowerCase)) {
        map += key.toLowerCase -> arr.indexOf(key)
    })
    map
  }

  // get hive table column names
  private def getColNames(sparkSession: SparkSession): List[String] = {
    sparkSession
      .sql("desc " + conf.getString("database") + "." + conf.getString("table"))
      .toDF()
      .select("col_name")
      .filter("data_type not in('','data_type') and col_name != 'pt'")
      .collect()
      .map(_.get(0).toString)
      .toList
  }
}
