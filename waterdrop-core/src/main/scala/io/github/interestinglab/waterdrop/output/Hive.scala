package io.github.interestinglab.waterdrop.output

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Hive extends BaseOutput {

  final val INPUT_TYPE = "input_type"
  final val DATABASE = "database"
  final val TABLE = "table"
  final val PATH = "path"
  final val SCHEMA_TABLE = "schema_table"
  final val SAVE_MODE = "save_mode"
  final val SERIALIZER = "serializer"
  final val TABLE_REGEX = "table_regex"
  final val COMPRESSION = "compression"

  var config: Config = ConfigFactory.empty

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    (config.hasPath(DATABASE) && config.getString(DATABASE).trim.nonEmpty) &&
      (config.hasPath(TABLE) && config.getString(TABLE).trim.nonEmpty) &&
      (config.hasPath(PATH) && config.getString(PATH).trim.nonEmpty) match {
      case true => {
        val path = config.getString(PATH)
        path.startsWith("/") match {
          case true => (true, "")
          case false => (false, "invalid path URI, it should be start with '/'")
        }
      }
      case false => (false, "please specify [input_type] [database] [table] [path] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        INPUT_TYPE -> "incre",
        SCHEMA_TABLE -> "bd.tbl_mysql_table_schema",
        SAVE_MODE -> "error",
        SERIALIZER -> "parquet",
        TABLE_REGEX -> ".*",
        COMPRESSION -> "gzip"
      )
    )

    config = config.withFallback(defaultConfig)

  }

  override def process(df: Dataset[Row]): Unit = {

    import df.sparkSession.implicits._

    val inputDf = df.filter($"mTableName".rlike(config.getString(TABLE_REGEX)))

    val outputDf = config.getString(INPUT_TYPE) match {
      case "incre" =>
        schemaDf(df.sparkSession, inputDf, config.getString(DATABASE), config.getString(TABLE))
      case "total" =>
        reSchemaDf(df.sparkSession, inputDf, config.getString(DATABASE), config.getString(TABLE))
      case _ => df.sparkSession.emptyDataFrame
    }

    // convert json to row text with delimiter

    outputDf.write
      .mode(config.getString(SAVE_MODE))
      .option(COMPRESSION, config.getString(COMPRESSION))
      .option(SERIALIZER, config.getString(SERIALIZER))
      .save(config.getString(PATH))

  }

  private def getTableSchema(
    sparkSession: SparkSession,
    df: Dataset[Row],
    database: String,
    table: String): StructType = {
    val schemaTable = config.getString(SCHEMA_TABLE)
    val schemaStr =
      sparkSession
        .sql(s"SELECT mysqlschema FROM $schemaTable WHERE tablename = '$database.$table'")
        .collect()(0)
        .getString(0)

    val schemaJson = JSON.parseObject(schemaStr)
    var schema = new StructType()
    schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)
    schema
  }

  private def schemaDf(sparkSession: SparkSession, df: Dataset[Row], database: String, table: String): Dataset[Row] = {

    val schema = getTableSchema(sparkSession, df, database, table)

    var dataFrame = df.withColumn(RowConstant.TMP, from_json(col("fields"), schema))
    schema.foreach(f => {
      dataFrame = dataFrame.withColumn(f.name, col(RowConstant.TMP)(f.name))
    })

    dataFrame.drop(RowConstant.TMP, "fields")

  }

  private def reSchemaDf(
    sparkSession: SparkSession,
    df: Dataset[Row],
    database: String,
    table: String): Dataset[Row] = {

    val schema_mapping = getTableSchema(sparkSession, df, database, table)
      .add("mDatabaseName", DataTypes.StringType)
      .add("mTableName", DataTypes.StringType)
      .add("mActionType", DataTypes.StringType)
      .add("mActionTime", DataTypes.LongType)
      .map(s => s.name -> s.dataType)
      .toMap

    df.select(df.columns.map { c =>
      col(c).cast(schema_mapping(c))
    }: _*)
  }

}
