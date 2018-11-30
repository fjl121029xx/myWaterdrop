package io.github.interestinglab.waterdrop.input

import java.util
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Jdbc extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "jdbc" -> "json"
      )
    )

    this.config = config.withFallback(defaultConfig)
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    // TODO: are user, password required ?
    val requiredOptions = List("driver", "url", "query", "user", "password")
    //判断配置是否齐全
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    nonExistsOptions.length match {
      case 0 => (true, "")
      case _ =>
        (
          false,
          "please specify " + nonExistsOptions
            .map { option =>
              val (name, exists) = option
              "[" + name + "]"
            }
            .mkString(", ") + " as non-empty string"
        )
    }

  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val format = config.getString("jdbc")
    val reader = spark.read.format(format)

    showJdbcConf()

    val queryConfig = config.getConfig("query")
    val properties: Properties = getProperties

    queryConfig.getString("type") match {
      case "table" =>
        val frame: Dataset[Row] = reader.jdbc(config.getString("url"), queryConfig.getString("table"), properties)
        frame
      case "sql" =>
        val q_sql = "(" + queryConfig.getString("table") + ")" + " " + "t"
        val frame: Dataset[Row] = reader.jdbc(config.getString("url"), q_sql, properties)
        frame
    }

  }

  def getProperties: Properties = {
    val properties: Properties = new Properties()
    properties.setProperty("driver", config.getString("driver"))
    properties.setProperty("user", config.getString("user"))
    properties.setProperty("password", config.getString("password"))
    properties
  }

  def showJdbcConf(): Unit = {
    val JdbcQueryParams: util.Map[String, Object] = mapAsJavaMap[String, Object](
      config
        .entrySet()
        .foldRight(Map[String, String]())((entry, map) => {
          map + (entry.getKey -> entry.getValue.unwrapped().toString)
        }))

    println("[INFO] Input JDBC Params:")

    for (entry <- JdbcQueryParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

}
