package io.github.interestinglab.waterdrop.apis

import java.util

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

abstract class BaseOutput extends Plugin with Runnable {

  var df: Dataset[Row] = _

  var filterWrapper: FilterWrapper = new FilterWrapper
  //  var correct_accumulator: LongAccumulator = _
  //  var error_accumulator: LongAccumulator = _
  //  var sum_accumulator: LongAccumulator = _

  var accu_map: util.HashMap[String, LongAccumulator] = _

  def process(df: Dataset[Row]): Unit = {}

  //=== 20191029
  def processWithMetrics(df: Dataset[Row], accu_map: util.HashMap[String, LongAccumulator]): Unit = {}

  override def prepare(spark: SparkSession): Unit = {

    if (getConfig().hasPath("filters")) {
      filterWrapper.initFilters(getConfig().getString("filters"))
    }
  }

  //=== 20191029
  override def prepareWithMetrics(spark: SparkSession, accu_map: util.HashMap[String, LongAccumulator]): Unit = {
    if (getConfig().hasPath("filters")) {
      filterWrapper.initFilters(getConfig().getString("filters"))
    }
    this.accu_map = accu_map
  }

  def filterProcess(df: Dataset[Row]): Dataset[Row] = {
    filterWrapper.processes(df)
  }

  def setDf(df: Dataset[Row]): Unit = {
    this.df = df
  }

  override def run(): Unit = {
    val fds = filterProcess(df)
    if (checkAccumulator(this.getClass.getSimpleName)) {
      this.process(fds)
    } else {
      this.processWithMetrics(fds, accu_map)
    }

  }

  def checkAccumulator(output_name: String): Boolean = {
    var i = 0
    if (accu_map != null) {
      val acIts = accu_map.keySet().iterator()
      while (acIts.hasNext) {
        val accu_name = acIts.next()
        if (accu_name.startsWith(output_name)) {
          i += 1
        }
      }
      i != 3
    } else {
      false
    }
  }
}
