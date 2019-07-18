package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.utils.StringTemplate
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat, lit}
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions._

class Elasticsearch extends BaseOutput {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es"

  var config: Config = ConfigFactory.empty()

  val config_index_id = "index_id"
  val config_index_type = "index_type"
  val config_index_schema = "index_schema"
  val config_index_filter_condition = "index_filter_condition"

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("hosts") && config.getStringList("hosts").size() > 0 match {
      case true => {
        val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [hosts] as a non-empty string list")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "index" -> "waterdrop",
        "index_type" -> "log",
        "index_time_format" -> "yyyy.MM.dd",
        "es.mapping.id" -> "waterdrop_es_mapping_id",
        "es.ingest.pipeline" -> "waterdrop_remove"
      )
    )
    config = config.withFallback(defaultConfig)

    config
      .getConfig(esPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        esCfg += (esPrefix + "." + key -> value)
      })

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))
  }

  override def process(df: Dataset[Row]): Unit = {

    //judge make id
    val dfwithid = config.hasPath("index_id") match {
      case true => {
        val cols = config
          .getString("index_id")
          .split(",")
          .map(filed => {
            if (filed.startsWith("$")) col(filed.substring(1, filed.length)) else lit(filed)
          })
        df.withColumn("waterdrop_es_mapping_id", concat(cols: _*))
      }
      case false => df
    }

    //judge has index suffix
    val (index, dff) = config.hasPath("index_suffix") match {
      case true => {
        val suffixes = config.getString("index_suffix").split(":")
        val indexName = config.getString("index") + "{waterdrop_es_index_suffix}"
        val dfwithSuffix = suffixes.length match {
          case 1 => dfwithid.withColumn("waterdrop_es_index_suffix", col(suffixes.apply(0)))
          case 3 =>
            dfwithid.withColumn(
              "waterdrop_es_index_suffix",
              col(suffixes.apply(0)).substr(suffixes.apply(1).toInt, suffixes.apply(2).toInt))
        }
        (indexName, dfwithSuffix)
      }
      case false =>
        (StringTemplate.substitute(config.getString("index"), config.getString("index_time_format")), dfwithid)
    }

    //save to es
    dff.saveToEs(index + "/" + config.getString("index_type"), this.esCfg)
  }
}
