package io.github.interestinglab.waterdrop.apis

import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, Row}


class FilterWrapper extends Serializable {

  var filters: List[BaseFilter] = List.empty

  val filterPath = "io.github.interestinglab.waterdrop.filter."

  def initFilters(filterString: String): Unit = {

    println(s"filters: $filterString")

    val filterInfoArray = JSON.parseArray(filterString).toArray

    for (j <- filterInfoArray) {
      val json = JSON.parseObject(j.toString)

      val key = json.keySet().toArray.apply(0).asInstanceOf[String]

      val filterClazz = Class.forName("%s%s".format(filterPath, key))

      val filter = filterClazz.newInstance().asInstanceOf[BaseFilter]

      filter.setConfig(ConfigFactory.parseMap(json.getJSONObject(key)))
      filter.prepare(null)

      filters = filters :+ filter
    }

    showFilter()
  }

  def processes(df: Dataset[Row]): Dataset[Row] = {

    val spark = df.sparkSession
    var tmpdf = df

    for (f <- filters) {
      tmpdf = f.process(spark, tmpdf)
    }
    tmpdf
  }


  def showFilter(): Unit = {

    println("filters:")
    for (f <- filters) {
      println(s"\tfilter -> ${f.getClass.getSimpleName}")
      println(s"\tconf -> ${f.getConfig()}")
    }
  }

}