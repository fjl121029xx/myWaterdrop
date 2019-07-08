package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseOutput extends Plugin {

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

}
