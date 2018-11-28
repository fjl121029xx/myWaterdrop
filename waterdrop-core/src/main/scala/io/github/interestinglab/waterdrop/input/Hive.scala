package io.github.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
    * Set Config.
    **/
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
    * Get Config.
    **/
  override def getConfig(): Config = {
    this.config
  }

  /**
    * Get Dataset from this Static Input.
    **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {

    spark.sql(buildSqlStr)

  }

  def buildSqlStr: String = {
    val query = config.getString("query")
    val database = config.getString("database")
    val table = config.getString("table")
    val columns = if (config.getString("columns").isEmpty) "*" else config.getString("columns")
    val where = if (config.getString("where").isEmpty) "" else "WHERE " + config.getString("where")
    val partition = if (config.getString("partition").isEmpty) "" else "AND " + config.getString("partition")

    val sqlStr = if (query.isEmpty) s"SELECT $columns FROM $database.$table $where $partition" else query

    sqlStr
  }

  /**
    * Return true and empty string if config is valid, return false and error message if config is invalid.
    */
  override def checkConfig(): (Boolean, String) = {

  }

}
