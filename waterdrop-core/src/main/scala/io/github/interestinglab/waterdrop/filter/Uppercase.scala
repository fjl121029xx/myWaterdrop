package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, upper}

import scala.collection.JavaConversions._

class Uppercase extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   **/
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   **/
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> "uppercased"
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    //    df.withColumn(conf.getString("target_field"), upper(col(conf.getString("source_field"))))
    val cols = df.columns
    val lookup = cols .map(f => (f, f.toUpperCase)).toMap
    df.select(df.columns.map(c => col(c).as(lookup.getOrElse(c, c))): _*)
  }
}
