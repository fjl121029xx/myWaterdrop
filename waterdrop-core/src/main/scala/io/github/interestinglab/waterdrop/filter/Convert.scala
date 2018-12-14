package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, concat, lit}

import scala.collection.JavaConversions._

class Convert extends BaseFilter {

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
    if (!conf.hasPath("source_field")) {
      (false, "please specify [source_field] as a non-empty string")
    } else {
      (true, "")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "value_fields" -> "", //字符串转换 $id 从字段获]取 0 则为常量
        "new_type" -> "string"
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val srcField = conf.getString("source_field")
    val newType = conf.getString("new_type")

    val cdf = conf.getString("value_fields") match {
          case "" => df
          case _ => {
            val cols = conf.getString("value_fields").split(",").map(filed => {
              if (filed.startsWith("$")) col(filed.substring(1,filed.length)) else lit(filed)
            })

        df.withColumn(conf.getString("source_field"),concat(cols :_*))
      }
    }

    newType match {
      case "string" => cdf.withColumn(srcField, col(srcField).cast(StringType))
      case "integer" => cdf.withColumn(srcField, col(srcField).cast(IntegerType))
      case "double" => cdf.withColumn(srcField, col(srcField).cast(DoubleType))
      case "float" => cdf.withColumn(srcField, col(srcField).cast(FloatType))
      case "long" => cdf.withColumn(srcField, col(srcField).cast(LongType))
      case "boolean" => cdf.withColumn(srcField, col(srcField).cast(BooleanType))
      case _: String => cdf
    }
  }
}
