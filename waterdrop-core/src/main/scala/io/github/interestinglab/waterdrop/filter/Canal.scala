package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.JSON
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
        "table.delete.option" -> false,
        "canal.field.include" -> true
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    import spark.implicits._

    //filter empty dataset
    if (df.count == 0) {
      println("[WARN] dataset is empty, return empty dataframe")
      spark.emptyDataFrame
    } else {
      //dataset pre-process,convert to dataframe
      val jsonDf = spark.read.json(df.mapPartitions(it => it.map(_.mkString)))

      //filter table name
      val table_regex = conf.getString("table.regex")
      val delete_option = conf.getBoolean("table.delete.option")
      val newDf = jsonDf.filter($"$TABLE_NAME".rlike(table_regex))

      //filter delete
      val jsonRDD = delete_option match {
        case true => newDf.toJSON.mapPartitions(canalFieldsExtract)
        case false => newDf.filter($"$ACTION_TYPE".notEqual("DELETE")).toJSON.mapPartitions(canalFieldsExtract)
      }
      spark.read.json(jsonRDD)
    }

  }

  //extract and put fields from json string
  private def canalFieldsExtract(it: Iterator[String]): Iterator[String] = {

    val lb = ListBuffer[String]()

    while (it.hasNext) {
      val next = JSON.parseObject(it.next)
      val source = next.getJSONObject(SOURCE_FIELD)

      conf.getBoolean("canal.field.include") match {
        case true => {
          source.put("mDatabaseName", next.getString(DATABASE_NAME))
          source.put("mTableName", next.getString(TABLE_NAME))
          source.put("mActionType", next.getString(ACTION_TYPE))
          source.put("mActionTime", next.getString(TS).split(",")(0))
        }
        case false => //do nothing
      }
      lb.append(source.toString)
    }

    lb.toIterator
  }

}
