package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Canal extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "table.regex" -> ".*",
        "table.delete.option" -> false
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    import spark.implicits._

    val tableRegex = conf.getString("table.regex")

    //filter table and delete
    val canalDf = conf.getBoolean("table.delete.option") match {
      case true =>
        df.filter($"tableName".rlike(tableRegex))
      case false =>
        df.filter($"tableName".rlike(tableRegex))
          .filter($"actionType".notEqual("DELETE"))

    }

    canalDf
      .withColumn("mActionTime", monotonically_increasing_id)
      .drop("updatedOriginalFields", "updatedFields", "primaryKeyName", "ts")

  }

}
