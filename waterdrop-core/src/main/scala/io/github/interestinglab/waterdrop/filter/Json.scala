package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}

class Json extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
    * Set Config.
    **/
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
    * Get Config.
    **/
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    (true, "")
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val schema = new StructType()
      .add("raw_message", StringType)

    val hrdd = df.toJSON.rdd.map(RowFactory.create(_))

    spark.createDataset(hrdd)(RowEncoder(schema))
  }
}
