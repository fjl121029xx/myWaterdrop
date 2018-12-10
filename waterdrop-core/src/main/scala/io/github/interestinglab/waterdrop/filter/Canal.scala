package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Canal extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  val SOURCE_FIELD = "fields"
  val TABLE_NAME = "tableName"
  val ACTION_TYPE = "actionType"

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

    val table_regex = conf.getString("table.regex")
    val delete_option = conf.getBoolean("table.delete.option")
    val newDf = df.filter($"$TABLE_NAME".rlike(table_regex))

    val jsonRDD = delete_option match {
      case true => newDf.toJSON.mapPartitions(canalFieldsExtract)
      case false => newDf.filter($"$ACTION_TYPE".notEqual("DELETE")).toJSON.mapPartitions(canalFieldsExtract)
    }
    spark.read.json(jsonRDD)
  }

  private def canalFieldsExtract(it: Iterator[String]): Iterator[String] = {

    val lb = ListBuffer[String]()

    while (it.hasNext) {
      val next = JSON.parseObject(it.next)
      val source = next.getJSONObject(SOURCE_FIELD)
      val databaseName = next.getString("databaseName")
      val actionType = next.getString("actionType")
      val mActionTime = next.getString("ts").split(",")(0)
      source.put("databaseName", databaseName)
      source.put("actionType", actionType)
      source.put("mActionTime", mActionTime)
      lb.append(source.toString)
    }

    lb.toIterator
  }

}
