package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
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

    val schemaLost = unionFieldArr
      .map(field => {
        (field, df.schema.fieldNames.contains(field))
      })
      .filter(!_._2)

    if (schemaLost.size > 0) {
      println("[ERROR] union field missing!!!")
      spark.emptyDataFrame
    } else {
      //add increasing unionKey
      val increasingID = "datasets_increasing_id"
      val unionKey = "datasets_union_key"

      val dfi = df
        .withColumn(increasingID, monotonically_increasing_id)
        .withColumn(unionKey, concat(unionFieldArr.map(col(_)): _*))

      //get recent data
      val dfm = dfi
        .groupBy(unionKey)
        .max(increasingID)

      //drop increasing and unionKey
      dfm
        .join(dfi, dfm(s"max($increasingID)") === dfi(increasingID) and dfm(unionKey) === dfi(unionKey))
        .drop(s"max($increasingID)", increasingID, unionKey)
    }
  }

}
