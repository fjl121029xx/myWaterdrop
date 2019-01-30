package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Sql extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("table_name") && conf.hasPath("sql") match {
      case true => (true, "")
      case false => (false, "please specify [table_name] and [sql]")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    import spark.implicits._

    val schemaFileds = df.schema.fields.toList

    val cdf = (schemaFileds.size == 1 && ("raw_message").equals(schemaFileds.apply(0).name)) match {
      case true => spark.read.json(df.mapPartitions(it => it.map(_.mkString)))
      case false => df
    }

    cdf.createOrReplaceTempView(this.conf.getString("table_name"))
    spark.sql(conf.getString("sql"))
  }
}
