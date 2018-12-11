package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Canal extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  //fields name
  val SOURCE_FIELD = "fields"
  val DATABASE_NAME = "databaseName"
  val TABLE_NAME = "tableName"
  val ACTION_TYPE = "actionType"
  val TS = "ts"
  val M_ACTION_TIME = "mActionTime"

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

  //extract and put fields from json string
  private def canalFieldsExtract(it: Iterator[String]): Iterator[String] = {

    val lb = ListBuffer[String]()

    while (it.hasNext) {
      val next = JSON.parseObject(it.next)
      val source = next.getJSONObject(SOURCE_FIELD)
      val databaseName = next.getString(DATABASE_NAME)
      val tableName = next.getString(TABLE_NAME)
      val actionType = next.getString(ACTION_TYPE)
      val mActionTime = next.getString(TS).split(",")(0)
      source.put(DATABASE_NAME, databaseName)
      source.put(TABLE_NAME, tableName)
      source.put(ACTION_TYPE, actionType)
      source.put(M_ACTION_TIME, mActionTime)
      lb.append(source.toString)
    }

    lb.toIterator
  }

}
