package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

abstract class BaseOutput extends Plugin with Runnable {

  var df: Dataset[Row] = _

  var filterWrapper: FilterWrapper = new FilterWrapper
  var correct_accumulator: LongAccumulator = _
  var error_accumulator: LongAccumulator = _
  var sum_accumulator: LongAccumulator = _

  def process(df: Dataset[Row]): Unit = {}

  //=== 20191029
  def processWithMetrics(df: Dataset[Row], correct: LongAccumulator, error: LongAccumulator, sum: LongAccumulator): Unit = {}

  override def prepare(spark: SparkSession): Unit = {

    if (getConfig().hasPath("filters")) {
      filterWrapper.initFilters(getConfig().getString("filters"))
    }
  }

  //=== 20191029
  override def prepareWithMetrics(spark: SparkSession, correct: LongAccumulator, error: LongAccumulator, sum: LongAccumulator): Unit = {
    if (getConfig().hasPath("filters")) {
      filterWrapper.initFilters(getConfig().getString("filters"))
    }
    correct_accumulator = correct
    error_accumulator = error
    sum_accumulator = sum
  }

  def filterProcess(df: Dataset[Row]): Dataset[Row] = {
    filterWrapper.processes(df)
  }

  def setDf(df: Dataset[Row]): Unit = {
    this.df = df
  }

  override def run(): Unit = {
    val fds = filterProcess(df)
    if (checkAccumulator) {
      this.process(fds)
    } else {
      this.processWithMetrics(fds, correct_accumulator, error_accumulator, sum_accumulator)
    }

  }

  def checkAccumulator: Boolean = {
    correct_accumulator == null && error_accumulator == null && sum_accumulator == null
  }


}
