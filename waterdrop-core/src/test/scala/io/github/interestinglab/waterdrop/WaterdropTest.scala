package io.github.interestinglab.waterdrop

import java.util.concurrent.Future

import com.alibaba.fastjson.JSON
import io.github.interestinglab.waterdrop.apis.FilterWrapper
import io.github.interestinglab.waterdrop.utils.{MysqlWriter, SchemaUtils, SparkSturctTypeUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.junit.Test


class WaterdropTest {

  @Test def naFillTest(): Unit = {

    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

    val df = spark.read.json("/Users/jiaquanyu/tmp/1.txt")

    var tmpdf = df.na.fill(Map("age" -> 0, "add" -> ""))
    tmpdf.createOrReplaceTempView("tbl_tmp")

    tmpdf = spark.sql("select * from tbl_tmp")
    tmpdf.show()

  }

  @Test def getStructTypeTest (): Unit={

    val jsonObject = JSON.parseObject("{\"deviceName\" : \"string\", \"foodRealPrice\" : \"decimal\", \"foodPayPrice\" : \"decimal\", " +
      "\"sendBy\" : \"string\", \"allFoodRemark\" : \"string\", \"foodSubType\" : \"string\", \"areaName\" : \"string\", \"foodSubjectCode\" : \"string\"," +
      " \"isDiscount\" : \"integer\", \"serveConfirmTime\" : \"long\", \"setFoodCategoryName\" : \"string\", \"salesCommission\" : \"decimal\"," +
      " \"makeEndNumber\" : \"decimal\", \"startTime\" : \"long\", \"departmentKeyLst\" : \"string\", \"foodCode\" : \"string\", \"actionTime\" : \"long\"," +
      " \"shopID\" : \"long\", \"foodCategorySortIndex\" : \"integer\", \"saasOrderKey\" : \"string\", \"makeCallCount\" : \"integer\", " +
      "\"isDiscountDefault\" : \"integer\", \"foodRealAmount\" : \"decimal\", \"itemKey\" : \"string\", \"createTime\" : \"long\", " +
      "\"readyNumber\" : \"decimal\", \"foodTaste\" : \"string\", \"shiftName\" : \"string\", \"parentFoodFromItemKey\" : \"string\"," +
      " \"foodLastCancelNumber\" : \"decimal\", \"foodKey\" : \"string\", \"itemID\" : \"string\", \"taxRate\" : \"decimal\", " +
      "\"foodPriceAmount\" : \"decimal\", \"foodEstimateCost\" : \"decimal\", \"cancelReason\" : \"string\", \"orderSubType\" : \"integer\"," +
      " \"orderStatus\" : \"integer\", \"groupID\" : \"long\", \"cancelBy\" : \"string\", \"timeNameCheckout\" : \"string\", \"setFoodRemark\" : \"string\", " +
      "\"foodProPrice\" : \"decimal\", \"makeStatus\" : \"integer\", \"foodRemark\" : \"string\", \"foodCategoryName\" : \"string\", " +
      "\"isNeedConfirmFoodNumber\" : \"integer\", \"isSFDetail\" : \"integer\", \"clientType\" : \"string\", \"promotionIDLst\" : \"string\", " +
      "\"foodVipPrice\" : \"decimal\", \"makeBy\" : \"string\", \"modifyTime\" : \"long\", \"isBatching\" : \"integer\", \"tableName\" : \"string\"," +
      " \"sendTime\" : \"long\", \"sendReason\" : \"string\", \"brandID\" : \"long\", \"orderBy\" : \"string\", \"foodPractice\" : \"string\", " +
      "\"foodNumber\" : \"decimal\", \"transmitNumber\" : \"decimal\", \"foodCancelNumber\" : \"decimal\", \"modifyBy\" : \"string\"," +
      " \"foodSubjectName\" : \"string\", \"unitKey\" : \"string\", \"foodCategoryKey\" : \"string\", \"serverMAC\" : \"string\", " +
      "\"isSetFood\" : \"integer\", \"makeEndfoodNumber\" : \"decimal\", \"categoryDiscountRate\" : \"decimal\", \"shopName\" : \"string\"," +
      " \"unit\" : \"string\", \"foodSubjectKey\" : \"string\", \"foodDiscountRate\" : \"decimal\", \"departmentKeyOne\" : \"string\"," +
      " \"foodPayPriceReal\" : \"decimal\", \"cancelTime\" : \"long\", \"modifyReason\" : \"string\", \"makeEndTime\" : \"long\", " +
      "\"unitAdjuvantNumber\" : \"decimal\", \"makeStartTime\" : \"long\", \"reportDate\" : \"long\", \"foodSendNumber\" : \"decimal\"," +
      " \"checkoutTime\" : \"long\", \"setFoodName\" : \"string\", \"action\" : \"integer\", \"unitAdjuvant\" : \"string\", \"foodName\" : \"string\"," +
      " \"actionBatchNo\" : \"string\", \"serveConfirmBy\" : \"string\", \"isTempFood\" : \"integer\", \"printStatus\" : \"integer\"}\n")
    System.out.println(SparkSturctTypeUtil.getStructType(new StructType, jsonObject))
  }

  @Test def getSchemaStringTest: Unit = {

    val mysqlWriter = MysqlWriter("jdbc:mysql://192.168.22.63/test", "extractor", "extractor")
    val colWithDataType = mysqlWriter.getColWithDataType("test", "tbl_crm_trans_detail")
    println(SchemaUtils.getSchemaString(colWithDataType))
    val defaultMap = mysqlWriter.getTableDefaultValue("test","tbl_crm_trans_detail")
    println(defaultMap)

    val map = collection.mutable.Map(defaultMap.toSeq: _*).map(kv => {
      if ("CURRENT_TIMESTAMP".equals(kv._2)) {
        (kv._1, System.currentTimeMillis())
      } else {
        (kv._1, kv._2)
      }
    })
    println(map)

    println(defaultMap)

  }

  @Test def filterWrapperTest(): Unit = {

    val filtersStr = "[{\"Schema\":{\"schema\":{\"name\":\"string\",\"address\":\"string\"}}}," +
      "{\"Schema\":{\"schema\":{\"name\":\"string\",\"id\":\"string\"}}},{\"Schema\":{\"schema\":{\"name\":\"string\",\"id2\":\"string\"}}}]"

    val filterWrapper = new FilterWrapper()
    filterWrapper.initFilters(filtersStr)
  }

  @Test def filterWrapperProcessTest(): Unit = {

    val sqlContext = SparkSession.builder().appName("Spark Sql Test").master("local").getOrCreate()

    val url = "jdbc:mysql://192.168.22.63:3306/test?user=extractor&password=extractor"
    val df = sqlContext.read.format("jdbc").option("url", url).option("dbtable", "person").load()

    val filterStr = "[{\"Remove\":{\"source_field\":[\"age\"]}},{\"Remove\":{\"source_field\":[\"sex\",\"actionStamp\"]}}]"

    val filterWrapper = new FilterWrapper()
    filterWrapper.initFilters(filterStr)

    val rdf = filterWrapper.processes(df)

    rdf.show()
  }

  @Test def mapTest():Unit ={
    val map:Map[String,String] = Map("1"->"a","2"->"b")

    val mmap = collection.mutable.Map(map.toSeq: _*)
    mmap += ("3"->"c")

    println(map)
    println(mmap)
  }

}
