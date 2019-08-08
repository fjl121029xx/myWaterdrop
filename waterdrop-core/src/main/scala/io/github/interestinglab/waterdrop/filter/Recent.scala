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
    process(df)
  }

  def process(df: Dataset[Row]): Dataset[Row] = {

    val unionFieldArr = conf.getString("union.fields").split(",")

    val windowSpec = Window.partitionBy(unionFieldArr.map(col(_)): _*)

    conf.hasPath("recent_field") match {
      case true => {
        val recent_field = conf.getString("recent_field")
          df.withColumn(s"max_$recent_field", max(col(recent_field)).over(windowSpec))
          .where(s"$recent_field == max_$recent_field")
          .drop( s"max_$recent_field")
      }
      case false => {
        val increasingID = "df_increasing_id"

        df.withColumn(increasingID, monotonically_increasing_id)
          .withColumn(s"max_$increasingID", max(col(increasingID)).over(windowSpec))
          .where(s"$increasingID == max_$increasingID")
          .drop(increasingID, s"max_$increasingID")
      }
    }
  }

}
