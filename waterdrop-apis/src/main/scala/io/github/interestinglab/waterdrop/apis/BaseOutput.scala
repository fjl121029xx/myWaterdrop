package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseOutput extends Plugin with Runnable {

  var df: Dataset[Row] = _

  var filterWrapper: FilterWrapper = new FilterWrapper

  def process(df: Dataset[Row]): Unit = {}

  override def prepare(spark: SparkSession): Unit = {

    if (getConfig().hasPath("filters")) {
      filterWrapper.initFilters(getConfig().getString("filters"))
    }
  }

  def filterProcess(df: Dataset[Row]): Dataset[Row] = {
    filterWrapper.processes(df)
  }

  def setDf(df: Dataset[Row]): Unit = {
    this.df = df
  }

  override def run(): Unit = {
    val fds = filterProcess(df)
    this.process(fds)
  }
}
