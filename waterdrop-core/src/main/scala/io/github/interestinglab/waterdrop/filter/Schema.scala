package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class Schema extends BaseFilter {

  var conf: Config = ConfigFactory.empty()
  var schema = new StructType()

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = conf

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("schema") match {
      case true => (true, "")
      case false => (false, "please specify [schema] !!!")
    }

  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(Map("source" -> RowConstant.RAW_MESSAGE))
    conf = conf.withFallback(defaultConfig)
    val schemaJson = JSON.parseObject(conf.getString("schema"))
    schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    var dateFrame = df.withColumn(RowConstant.TMP, from_json(col(conf.getString("source")), schema))

    schema.foreach(f => {
      dateFrame = dateFrame.withColumn(f.name, col(RowConstant.TMP)(f.name))
    })

    dateFrame.drop(RowConstant.TMP, conf.getString("source"))
  }
}
