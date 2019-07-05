package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.utils.FilterWraper
import org.apache.spark.sql.SparkSession
import org.junit.Test


class FilterWraperTest {

  @Test def filterWraperTest(): Unit = {

    val fitersStr = "[{\"Schema\":{\"schema\":{\"name\":\"string\",\"address\":\"string\"}}}," + "{\"Schema\":{\"schema\":{\"name\":\"string\",\"id\":\"string\"}}}," + "{\"Schema\":{\"schema\":{\"name\":\"string\",\"id2\":\"string\"}}}]"

    val filterWraper = new FilterWraper()
    filterWraper.initFilters(fitersStr)

  }

  @Test def fiterWraperProcessTest(): Unit ={

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

    val filterWraper = new FilterWraper()
    filterWraper.initFilters(filterStr)

    val rdf = filterWraper.processes(df)

    rdf.show()


  }
}
