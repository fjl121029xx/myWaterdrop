package io.github.interestinglab.waterdrop.output

import java.sql.{DriverManager, Statement}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Mysql extends BaseOutput {

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

    //df schema fields
    val schemeFields = df.schema.fieldNames.toList

    //sql fields type
    val stringFields = List("varchar", "timestamp", "datetime")
    val dateFields = List("timestamp", "datetime")
    val table = config.getString("table")

    // mysql mysql connect
    val mysqlWriter = df.sparkSession.sparkContext
      .broadcast(MysqlWriter(config.getString("url"), config.getString("username"), config.getString("password")))

    //get mysql table col with type
    val colWithType = mysqlWriter.value.getColWithDataType(table)

    //get sql field info
    val fields = colWithType.map(_._1)
    val strFields = colWithType.filter(tp => stringFields.contains(tp._2)).map(_._1)
    val timeStampFields = list2String(colWithType.filter(tp => dateFields.contains(tp._2)).map(_._1))

    //df schema intersect sql col
    val filterFields = fields
      .map(field => {
        schemeFields.contains(field) || schemeFields.contains(field.toLowerCase) match {
          case true => field
          case false => ""
        }
      })
      .filter(!_.equals(""))

    val fieldStr = list2String(filterFields)

    val sqlPrefix = s"${config.getString("insert.mode")} INTO $table ($fieldStr) VALUES "

    df.foreachPartition(it => {
      var i = 0
      val sb = new StringBuffer
      while (it.hasNext) {

        val nextMap: Map[String, Any] = config.getBoolean("row.column.toLower") match {
          case true => it.next.getValuesMap(filterFields.map(_.toLowerCase))
          case false => it.next.getValuesMap(filterFields)
        }

        sb.append(s"(");
        filterFields.foreach(field => {

          schemeFields.contains(field) || schemeFields.contains(field.toLowerCase) match {
            case true => {

              val rowFieldValue = nextMap.get(getRowField(field)).get

              strFields.contains(field) && rowFieldValue != null match {
                case true => {

                  val value = (nextMap.contains(getRowField(field))) match {
                    case true =>
                      sb.append(
                        "\"" + rowFieldValue.toString
                          .replace("\\", "\\\\")
                          .replace("\"", "\\\"") + "\"")
                    case false => sb.append("\"\"")
                  }

                  if (timeStampFields.contains(field) && "".equals(value)) {
                    sb.append("\"2000-01-01 01:01:01\"")
                  }
                }
                case false => sb.append(rowFieldValue)
              }
              if (!fieldStr.endsWith(field)) sb.append(",")
            }
            case false => //do nothing
          }
        })

        sb.append("),")
        i = i + 1

        if (i == config.getInt("batch.count") || (!it.hasNext)) {

          val upsert = sqlPrefix + sb.toString.substring(0, sb.length() - 1)

          mysqlWriter.value.upsert(upsert)

          i = 0
          sb.delete(0, sb.length())
        }
      }
    })
  }

  private def getRowField(field: String) = if (config.getBoolean("row.column.toLower")) field.toLowerCase else field

  private def list2String(list: List[String]) = list.toString.substring(5, list.toString.length - 1).replace(" ", "")
}

class MysqlWriter(createWriter: () => Statement) extends Serializable {

  lazy val writer = createWriter()

  def getColWithDataType(tableName: String): List[Tuple2[String, String]] = {

    val schemaSql = s"SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '$tableName'"
    val rs = writer.executeQuery(schemaSql)

    val lb = new ListBuffer[Tuple2[String, String]]

    while (rs.next()) {
      lb.append((rs.getString(1), rs.getString(2)))
    }
    lb.toList
  }

  def upsert(sql: String): Unit = {
    try {
      writer.executeUpdate(sql)
    } catch {
      case ex: Exception =>
        println(sql)
        throw ex
    }
  }
}

object MysqlWriter {

  def apply(jdbc: String, username: String, password: String): MysqlWriter = {

    val f = () => {

      val conn = DriverManager.getConnection(jdbc, username, password)
      val statement = conn.createStatement()

      sys.addShutdownHook {
        conn.close()
        statement.close()
      }
      statement
    }
    new MysqlWriter(f)
  }
}
