package io.github.interestinglab.waterdrop.output

import java.sql.{DriverManager, Statement}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Mysql extends BaseOutput {

  var firstProcess = true

  var config: Config = ConfigFactory.empty()

  /**
    * Set Config.
    **/
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
    * Get Config.
    **/
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
        "insert.mode" -> "INSERT", // INSERT or REPLACE
        "field.exclude" -> ""
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {

    val stringFields = List("varchar", "timestamp", "datetime")
    val dateFields = List("timestamp", "datetime")

    val jdbc = config.getString("url")
    val username = config.getString("username")
    val password = config.getString("password")

    val table = config.getString("table")

    val mysqlWriter = df.sparkSession.sparkContext.broadcast(MysqlWriter(jdbc, username, password))

    val colWithType = mysqlWriter.value.getColWithDataType(table, config.getString("field.exclude"))

    val fields = colWithType.map(_._1)
    val fieldStr = list2String(fields)
    val strFields = colWithType.filter(tp => stringFields.contains(tp._2)).map(_._1)
    val timeStampFields = list2String(colWithType.filter(tp => dateFields.contains(tp._2)).map(_._1))

    val sqlPrefix = s"${config.getString("insert.mode")} INTO $table ($fieldStr) VALUES "

    df.foreachPartition(it => {
      var i = 0
      val sb = new StringBuffer
      while (it.hasNext) {

        val nextMap: Map[String, Any] = config.getBoolean("row.column.toLower") match {
          case true => it.next.getValuesMap(fields.map(_.toLowerCase))
          case false => it.next.getValuesMap(fields)
        }

        sb.append(s"(");
        fields.foreach(field => {

          strFields.contains(field) match {
            case true => {

              val value = (nextMap.contains(getRowField(field))) match {
                case true => sb.append("\"" + nextMap.get(getRowField(field)).get.toString.replace("\\", "\\\\").replace("\"", "\\\"") + "\"")
                case false => sb.append("\"\"")
              }

              if (timeStampFields.contains(field) && "".equals(value)) {
                sb.append("\"2000-01-01 01:01:01\"")
              }
            }
            case false => sb.append(nextMap.get(getRowField(field)).get)
          }
          if (!fieldStr.endsWith(field)) sb.append(",")
        })

        sb.append("),")
        i = i + 1

        if (i == config.getInt("batch.count") || (!it.hasNext)) {

          val upsert = sqlPrefix + sb.toString.substring(0, sb.length() - 1)

          try {
            mysqlWriter.value.upsert(upsert)
          } catch {
            case ex: Exception => println(upsert)
              throw ex
          }

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

  def getColWithDataType(tableName: String, exclude: String = ""): List[Tuple2[String, String]] = {

    val schemaSql = s"SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '$tableName'"
    val rs = writer.executeQuery(schemaSql)

    val lb = new ListBuffer[Tuple2[String, String]]

    while (rs.next()) {
      if (!exclude.split(",").contains(rs.getString(1))) {
        lb.append((rs.getString(1), rs.getString(2)))
      }

    }
    lb.toList
  }

  def upsert(sql: String): Unit = {
    try {
      writer.executeUpdate(sql)
    } catch {
      case ex: Exception => println(sql)
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
