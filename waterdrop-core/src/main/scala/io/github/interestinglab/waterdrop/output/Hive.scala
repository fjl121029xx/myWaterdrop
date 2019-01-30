package io.github.interestinglab.waterdrop.output

import java.text.SimpleDateFormat
import java.util.Calendar

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

  private def buildOutputPath(path: String): String = {
    val outputPath = hdfs_prefix + path + buildPatitionPath
    outputPath
  }

  private def buildPatitionPath(): String = {
    val partition = {
      if ("none".equals(conf.getString("partition"))) {
        ""
      } else {
        getPartitionStr(conf.getString("partition"))
      }
    }
    partition
  }

  private def getPartitionStr(pt: String): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR_OF_DAY, -1)
    val pattern = pt match {
      case "hour" => "yyyyMMddHH"
      case "day" => "yyyyMMdd"
    }
    val sdf = new SimpleDateFormat(pattern)
    val pt_str = "/pt=" + sdf.format(cal.getTime)
    pt_str
  }

  private def getColMatchMap(cols: List[String], arr: Array[String]): mutable.HashMap[String, Int] = {
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

    //filter empty dataset
    if (df.count() == 0) {
      println("[WARN] dataset is empty, do nothing")
    } else {
      val cols = getColNames(df.sparkSession)
      val out_path = buildOutputPath(conf.getString("path"))
      val delim = conf.getString("delimiter")

      // match binlog column names
      val matchMap = getColMatchMap(cols, df.columns)

      // convert json to row text with delimiter
      val out_ds = df.map(row => {
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

      val writer = out_ds.write.mode(conf.getString("save_mode"))
      writer.text(out_path)
    }

  }

  // get hive table column names
  private def getColNames(sparkSession: SparkSession): List[String] = {

    val db = conf.getString("database")
    val table = conf.getString("table")

    val colNameArr = sparkSession
      .sql("desc " + db + "." + table)
      .toDF()
      .select("col_name")
      .filter("data_type not in('','data_type') and col_name != 'pt'")
      .collect()

    colNameArr.map(_.get(0).toString).toList
  }
}
