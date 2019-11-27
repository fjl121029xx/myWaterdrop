package io.github.interestinglab.waterdrop.apis

import java.util

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

abstract class BaseOutput extends Plugin with Runnable {

  var df: Dataset[Row] = _

  var filterWrapper: FilterWrapper = new FilterWrapper

  var accumulators: util.HashMap[String, LongAccumulator] = _

  override def setAccuMap(accuMap: util.HashMap[String, LongAccumulator]): Unit = {
    this.accumulators = accuMap
  }

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
