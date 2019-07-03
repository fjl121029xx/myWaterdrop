package io.github.interestinglab.waterdrop.output

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Hive extends BaseOutput {

  val hdfs_prefix: String = "hdfs://"
  var conf: Config = ConfigFactory.empty

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("database") && !conf.getString("database").trim.isEmpty ||
      conf.hasPath("table") && !conf.getString("table").trim.isEmpty ||
      conf.hasPath("path") && !conf.getString("path").trim.isEmpty match {
      case true => {
        val path = conf.getString("path")
        path.startsWith("/") match {
          case true => (true, "")
          case false => (false, "invalid path URI, it should be start with '/'")
        }
      }
      case false => (false, "please specify [database] [table] [path] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "schema_table" -> "bd.tbl_mysql_table_schema",
        "save_mode" -> "error",
        "serializer" -> "parquet",
        "table_regex" -> ".*",
        "compression" -> "gzip"
      )
    )

    conf = conf.withFallback(defaultConfig)

  }

  override def process(df: Dataset[Row]): Unit = {

    import df.sparkSession.implicits._

    // resolve fields value from json to dataframe
    val schemaDF = getSchemaDF(df.sparkSession, df, conf.getString("database"), conf.getString("table"))

    // convert json to row text with delimiter
    schemaDF
      .filter($"mTableName".rlike(conf.getString("table_regex")))
      .write
      .mode(conf.getString("save_mode"))
      .option("compression", conf.getString("compression"))
      .parquet(conf.getString("path"))

  }

  private def getSchemaDF(
    sparkSession: SparkSession,
    df: Dataset[Row],
    database: String,
    table: String): Dataset[Row] = {

    val schemaTable = conf.getString("schema_table")
    val schemaStr =
      sparkSession
        .sql(s"SELECT mysqlschema FROM $schemaTable WHERE tablename = '$database.$table'")
        .collect()(0)
        .getString(0)

    val schemaJson = JSON.parseObject(schemaStr)
    var schema = new StructType()
    schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)

    var dataFrame = df.withColumn(RowConstant.TMP, from_json(col("fields"), schema))
    schema.foreach(f => {
      dataFrame = dataFrame.withColumn(f.name, col(RowConstant.TMP)(f.name))
    })

    dataFrame.drop(RowConstant.TMP, "fields")

  }
}
