package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Except  extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  val param_source_table = "source_table"
  val param_target_table = "target_table"

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath(param_source_table) && conf.hasPath(param_target_table) match {
      case true => (true, "")
      case false => (false, "please specify [source_table] and [target_table]!!!")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    spark.sql(s"select * from ${conf.getString(param_source_table)}")
      .except(spark.sql(s"select * from ${conf.getString(param_target_table)}"))
  }
}
