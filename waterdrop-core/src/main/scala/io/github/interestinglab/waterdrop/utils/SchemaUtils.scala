package io.github.interestinglab.waterdrop.utils

import scala.util.parsing.json.JSONObject

object SchemaUtils {

  def getSchemaString(colWithTypes: List[(String, String)]): String = {

    val map = colWithTypes.map(f => {
      (f._1, correctionDataType(f._2))
    }).toMap

    JSONObject(map).toString
  }

  private def correctionDataType(dataType: String): String = {
    dataType match {
      case "tinyint" | "smallint" | "mediumint" | "int" | "bit" => "integer"
      case "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "varbinary" => "string"
      case "decimal" | "double" => "double"
      case "date" | "time" | "datetime" | "timestamp" => "timestamp"
      case "bigint" => "long"
    }
  }

}
