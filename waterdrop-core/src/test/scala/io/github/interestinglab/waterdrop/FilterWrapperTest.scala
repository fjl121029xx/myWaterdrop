package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.apis.FilterWrapper
import org.apache.spark.sql.SparkSession
import org.junit.Test


class FilterWrapperTest {

  @Test def filterWrapperTest(): Unit = {

    val filtersStr = "[{\"Schema\":{\"schema\":{\"name\":\"string\",\"address\":\"string\"}}}," + "{\"Schema\":{\"schema\":{\"name\":\"string\",\"id\":\"string\"}}}," + "{\"Schema\":{\"schema\":{\"name\":\"string\",\"id2\":\"string\"}}}]"

    val filterWrapper = new FilterWrapper()
    filterWrapper.initFilters(filtersStr)

  }

  @Test def filterWrapperProcessTest(): Unit ={

    val sqlContext = SparkSession.builder()
      .appName("Spark Sql Test")
      .master("local")
      .getOrCreate()

    val url =
      "jdbc:mysql://192.168.22.63:3306/test?user=extractor&password=extractor"
    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "person")
      .load()

    val filterStr = "[{\"Remove\":{\"source_field\":[\"age\"]}},{\"Remove\":{\"source_field\":[\"sex\",\"actionStamp\"]}}]"

    val filterWrapper = new FilterWrapper()
    filterWrapper.initFilters(filterStr)

    val rdf = filterWrapper.processes(df)

    rdf.show()


  }
}
