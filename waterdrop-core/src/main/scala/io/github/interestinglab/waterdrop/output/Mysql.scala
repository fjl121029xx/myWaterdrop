package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.Waterdrop
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.utils.{MysqlWriter, Retryer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Mysql extends BaseOutput {

  //sql fields type
  val stringFields = List("varchar", "timestamp", "datetime", "text", "varbinary", "longtext")
  val dateFields = List("timestamp", "datetime")

  var schemeFields, filterFields, strFields, timeStampFields = List.empty[String]

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "table", "username", "password")

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length > 0) {
      (false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    }
    (true, "")
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "driver" -> "com.mysql.jdbc.driver", // allowed values: overwrite, append, ignore, error
        "row.column.toLower" -> false, // convert dataset row column to lower
        "batch.count" -> 100, // insert batch  count
        "insert.mode" -> "INSERT" // INSERT or REPLACE
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {

    val dfFill = df.na.fill("").na.fill(0L).na.fill(0).na.fill(0.0)

    //df schema fields
    schemeFields = dfFill.schema.fieldNames.toList
    val table = config.getString("table")

    // mysql mysql connect
    val mysqlWriter = df.sparkSession.sparkContext
      .broadcast(MysqlWriter(config.getString("url"), config.getString("username"), config.getString("password")))

    //get mysql table col with type
    val colWithType = mysqlWriter.value.getColWithDataType(config.getString("url").split("/").last, table)

    //get sql field info
    val fields = colWithType.map(_._1)
    strFields = colWithType.filter(tp => stringFields.contains(tp._2)).map(_._1)
    timeStampFields = colWithType.filter(tp => dateFields.contains(tp._2)).map(_._1)

    //df schema intersect sql col
    filterFields = fields.map(field => {
      containsIgnoreCase(schemeFields, field) match {
        case true => field
        case false => ""
      }
    }).filter(!_.equals(""))

    val sum = dfFill.sparkSession.sparkContext.longAccumulator

    val sqlPrefix = s"${config.getString("insert.mode")} INTO $table ${filterFields.mkString("(",",",")")} VALUES "

    dfFill.foreachPartition(it => {
      var i = 0
      val sb = new StringBuffer
      while (it.hasNext) {
        sb.append(s"(")
        addSqlRow(sb, it.next())
        sb.append("),")
        i = i + 1

        if (i == config.getInt("batch.count") || (!it.hasNext)) {
          //写入mysql
          new Retryer().execute(mysqlWriter.value.upsert(sqlPrefix + sb.toString.substring(0, sb.length() - 1)))
          sum.add(i)

          i = 0
          sb.delete(0, sb.length())
        }
      }
    })

    println("[INFO] mysql out put: " + sum.value)

    if (Waterdrop.outputWritten == 0) Waterdrop.outputWritten = sum.value
  }

  private val containsIgnoreCase = (schemeFields: List[String], field: String) => schemeFields.contains(field) || schemeFields.contains(field.toLowerCase)

  private val getRowField = (field: String) => if (config.getBoolean("row.column.toLower")) field.toLowerCase else field


  private def addSqlRow(sb: StringBuffer, next: Row): Unit = {
    filterFields.foreach(field => {
      containsIgnoreCase(schemeFields, field) match {
        case true => strFields.contains(field) match {
          case true => val value = schemeFields.contains(getRowField(field)) match {
            case true => sb.append("\"" + next.getAs(getRowField(field)).toString.replace("\\", "\\\\").replace("\"", "\\\"").replace("\'", "\\'") + "\"")
            case false => sb.append("\"\"")
          } // 时间戳字段 空值处理
            if (timeStampFields.contains(field) && "".equals(value)) {
              sb.append("\"2000-01-01 01:01:01\"")
            }
          case false => sb.append(next.getAs(field).toString)
        }
          if (!filterFields.last.equals(field)) sb.append(",")
        case false => //do nothing
      }
    })
  }


}
