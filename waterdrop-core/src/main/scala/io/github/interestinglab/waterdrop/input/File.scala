package io.github.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * HDFS Static Input to read hdfs files in csv, json, parquet, parquet format.
 * */
class File extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "format" -> "json"
      )
    )

    this.config = config.withFallback(defaultConfig)
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    this.config.hasPath("path") match {
      case true => (true, "")
      case false => (false, "please specify [path] as string")
    }
  }

  private def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }

    path
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val format = config.getString("format")
    var reader = spark.read.format(format)

    Try(config.getConfig("options")) match {

      case Success(options) => {
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader = reader.options(optionMap)
      }
      case Failure(exception) => // do nothing

    }

    val path = buildPathWithDefaultSchema(config.getString("path"), "file://")

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case _ => reader.load(path)
    }
  }
}
