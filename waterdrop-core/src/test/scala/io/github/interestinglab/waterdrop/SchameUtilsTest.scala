package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.utils.{MysqlWriter, SchemaUtils}
import org.junit.Test


/**
  * @author jiaquanyu 
  */
//noinspection ScalaStyle
@Test
class SchameUtilsTest {

  @Test
  def getSchemaStringTest: Unit = {

    val mysqlWriter = MysqlWriter("jdbc:mysql://192.168.22.63/test","extractor","extractor")
    val colWithDataType = mysqlWriter.getColWithDataType("test","tbl_crm_trans_detail")
    println(SchemaUtils.getSchemaString(colWithDataType))

  }
}