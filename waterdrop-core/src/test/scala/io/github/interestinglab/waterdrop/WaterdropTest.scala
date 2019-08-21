package io.github.interestinglab.waterdrop

import org.junit.Test
import org.apache.spark.sql.SparkSession


class WaterdropTest {

  @Test def naFillTest(): Unit = {


    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("/Users/jiaquanyu/tmp/1.txt")

    var tmpdf = df.na.fill(Map("age" -> 0,"add"->"beijing"))
    tmpdf.show()

  }

}
