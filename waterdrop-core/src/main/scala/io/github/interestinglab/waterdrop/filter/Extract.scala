package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Extract extends BaseFilter {

  var conf: Config = ConfigFactory.empty()
  var schema = new StructType()

  val param_source = "source"
  val param_field = "field"

  var source: String = _
  var field: String = _

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = conf

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("field") match {
      case true => (true, "")
      case false => (false, "please specify [field] !!!")
    }

  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    prepare()
  }

  def prepare(): Unit = {

    val defaultConfig = ConfigFactory.parseMap(Map("source" -> RowConstant.RAW_MESSAGE))
    conf = conf.withFallback(defaultConfig)

    source = conf.getString(param_source)
    field = conf.getString(param_field)

    schema = StructType(StructField(field, ArrayType(
      SparkSturctTypeUtil.getStructType(schema, JSON.parseObject(conf.getString("schema"))))) :: Nil)

    println(s"extractor schema: $schema")
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    process(df)
  }

  def process(df: Dataset[Row]): Dataset[Row] = {

    df.withColumn(source, from_json(col(source), schema))
      .select(explode(col(source)(field)).as(field))
      .select(col(s"$field.*"))
  }

}
