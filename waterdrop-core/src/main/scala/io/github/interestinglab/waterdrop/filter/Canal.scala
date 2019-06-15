package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Canal extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  //fields name
  val SOURCE_FIELD = "fields"
  val DATABASE_NAME = "databaseName"
  val TABLE_NAME = "tableName"
  val ACTION_TYPE = "actionType"
  val UNIX_TS = "unixTs"

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

    //dataset pre-process,convert to dataframe
    val convertDf = df.schema.size match {
      case 1 => spark.read.json(df.mapPartitions(it => it.map(_.mkString)))
      case _ => df
    }

    val tableRegex = conf.getString("table.regex")

    //filter table and delete
    conf.getBoolean("table.delete.option") match {
      case true =>
        spark.read.json(convertDf.filter($"$TABLE_NAME".rlike(tableRegex)).toJSON.mapPartitions(canalFieldsExtract))
      case false =>
        spark.read.json(
          convertDf
            .filter($"$TABLE_NAME".rlike(tableRegex))
            .filter($"$ACTION_TYPE".notEqual("DELETE"))
            .toJSON
            .mapPartitions(canalFieldsExtract))
    }

  }

  //extract and put fields from json string
  private def canalFieldsExtract(it: Iterator[String]): Iterator[String] = {

    var id = 0L //顺序数

    it.map(row => {

      val rowJ = JSON.parseObject(row)
      val source = rowJ.getJSONObject(SOURCE_FIELD)

      conf.getBoolean("canal.field.include") match {
        case true => {
          val sec = System.currentTimeMillis() / 1000
          val mActionTime = f"$sec$id%08d".toLong
          source.put("mDatabaseName", rowJ.getString(DATABASE_NAME))
          source.put("mTableName", rowJ.getString(TABLE_NAME))
          source.put("mActionType", rowJ.getString(ACTION_TYPE))
          source.put("mActionTime", mActionTime)
        }
        case false => //do nothing
      }
      id += 1L
      source.toJSONString
    })
  }

}
