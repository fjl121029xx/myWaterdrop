package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.utils.{MysqlWriter, SchemaUtils}
import org.junit.Test


/**
  * @author jiaquanyu 
  */
@Test
class SchameUtilsTest {

  @Test
  def getSchemaStringTest: Unit = {

    val mysqlWriter = MysqlWriter("jdbc:mysql://192.168.22.63/test","extractor","extractor")
    val colWithDataType = mysqlWriter.getColWithDataType("test","tbl_saas_order_food")
    println(SchemaUtils.getSchemaString(colWithDataType))

  }
}