package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive extends BaseOutput {

  var conf = ConfigFactory.empty

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = ???

  //todo 1.解析时间 {day/hour} {hour:-1} {day:+1} yyyyMMdd 2.db table outPath 分隔符


  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  override def process(df: Dataset[Row]): Unit = {

    val cols = getColName(df.sparkSession)

  }

  private def getColName(sparkSession: SparkSession): List[String] = {

    val db = conf.getString("db")
    val table = conf.getString("table")

    val colNameArr = sparkSession.sql("desc " + db + "." + table)
      .toDF()
      .select("col_name").collect()

    colNameArr.map(_.get(0).toString).toList
  }
}
