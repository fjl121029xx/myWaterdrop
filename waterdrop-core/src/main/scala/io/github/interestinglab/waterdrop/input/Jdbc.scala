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
        "format" -> "json",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    )

    this.config = config.withFallback(defaultConfig)
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("host", "database", "username", "password", "query")
    //判断配置是否齐全
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    nonExistsOptions.length match {
      case 0 =>
        config.getString("query.type") match {
          case "table" =>
            if (config.hasPath("query.table") && !config.getString("query.table").isEmpty) {
              (true, "")
            } else {
              (false, "please specify [query.table] as non-empty")
            }
          case "sql" =>
            if (config.hasPath("query.sql") && !config.getString("query.sql").isEmpty) {
              (true, "")
            } else {
              (false, "please specify [query.sql] as non-empty")
            }
          case _ => (false, "please specify [query.type] as \"table\" or \"sql\"")
        }
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

    val format = config.getString("format")
    val reader = spark.read.format(format)

    showJdbcConf()

    val host = config.getString("host")
    val database = config.getString("database")
    val url =
      s"jdbc:mysql://$host/$database?tinyInt1isBit=false&zeroDateTimeBehavior=convertToNull&autoReconnect=true&serverTimezone=Asia/Shanghai"
    val properties: Properties = getProperties

    config.getString("query.type") match {
      case "table" =>
        if (config.hasPath("query.where")) {
          reader.jdbc(url, config.getString("query.table"), Array(config.getString("query.where")), properties)
        } else {
          reader.jdbc(url, config.getString("query.table"), properties)
        }
      case "sql" =>
        val sql = "(" + config.getString("query.sql") + ") t"
        reader.jdbc(url, sql, properties)
    }

  }

  def getProperties: Properties = {
    val properties: Properties = new Properties()
    properties.setProperty("driver", config.getString("driver"))
    properties.setProperty("user", config.getString("username"))
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
