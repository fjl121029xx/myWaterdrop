package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Recent extends BaseFilter {

  var conf = ConfigFactory.empty

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {

    conf.hasPath("union.fields") match {
      case true => (true, "")
      case false => (false, "union.fields is required !!!")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val unionFieldArr = conf.getString("union.fields").split(",")

    val increasingID = "datasets_increasing_id"

    val windowSpec = Window.partitionBy(unionFieldArr.map(col(_)): _*)

    df.withColumn(increasingID, monotonically_increasing_id)
      .withColumn(s"max_${increasingID}", max(col(increasingID)).over(windowSpec))
      .where(s"$increasingID == max_${increasingID}")
      .drop(increasingID, s"max_${increasingID}")
  }

}
