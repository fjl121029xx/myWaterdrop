package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Canal extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  val ACTION_TYPE = "actionType"

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig: (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "fields", //fileds source
        "table_name" -> "tableName", //table name
        "action_type" -> "actionType", //sql action type insert/update/delete
        "table.include" -> "*"
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    import spark.implicits._

    val newDf = conf.getString("table.include") match {
      case "*" => df
      case s: String => df.filter(conf.getString("table_name") + " in (\"" + s.replace(",", "\",\"") + "\")")
    }

    val jsonRDD = conf.hasPath("table.delete.option") match {
      case true => {
        val delOptions = JSON.parseObject(conf.getString("table.delete.option"))
        newDf.toJSON.mapPartitions(canalFiledsExtract(_,delOptions))
      }
      case false => {
        newDf.filter(conf.getString("action_type") + " != 'DELETE'")
          .toJSON.mapPartitions(canalFiledsExtract(_, null))
      }
    }
    spark.read.json(jsonRDD)
  }

  private def canalFiledsExtract(it: Iterator[String], delOptions: JSONObject): Iterator[String] = {

    val lb = ListBuffer[String]()

    delOptions == null match {
      case true => {
        while (it.hasNext) {
          lb.append((JSON.parseObject(it.next).getJSONObject(conf.getString("source_field")).toString))
        }
      }
      case false => {
        while (it.hasNext) {
          val next = JSON.parseObject(it.next)

          val source = next.getJSONObject(conf.getString("source_field"))

          (delOptions.containsKey(next.getString(conf.getString("table_name"))),
            next.getString(conf.getString("action_type")) == "DELETE") match {
            case (true, true) => {
              val delOption = delOptions.getJSONObject(next.getString(conf.getString("table_name")))
              delOption.keySet.foreach(key => source.put(key,delOption.get(key)))
              lb.append(source.toString)
            }
            case (false, true) => //filter delete
            case _ => lb.append(source.toString)
          }
        }
      }
    }

    lb.toIterator
  }
}