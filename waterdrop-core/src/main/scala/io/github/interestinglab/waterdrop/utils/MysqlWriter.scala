package io.github.interestinglab.waterdrop.utils

import java.sql.{Connection, DriverManager, Statement}

import scala.collection.mutable.ListBuffer

/**
  * @author jiaquanyu
  */
class MysqlWriter(createWriter: () => Statement) extends Serializable {

  lazy val writer = createWriter()

  def getColWithDataType(dbName: String, tableName: String): List[Tuple2[String, String]] = {

    val schemaSql = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
      s"WHERE table_name = '$tableName' AND table_schema = '${dbName}'"

    val rs = writer.executeQuery(schemaSql)

    val lb = new ListBuffer[Tuple2[String, String]]

    while (rs.next()) {
      lb.append((rs.getString(1), rs.getString(2)))
    }
    lb.toList
  }

  def getTableDefaultValue(dbName: String, tableName: String): Map[String, Object] = {

    val queryDefaultFields = "SELECT COLUMN_NAME,COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS " +
      s"WHERE IS_NULLABLE = 'NO' AND TABLE_NAME = '$tableName' AND table_schema = '$dbName' AND COLUMN_DEFAULT is not null"
    val rs = writer.executeQuery(queryDefaultFields)

    var map:Map[String,Object] = Map()
    while (rs.next()) map += ((rs.getString(1), rs.getObject(2)))

    map
  }

  def upsert(sql: String): Unit = {
    try {
      writer.executeUpdate(sql)
    } catch {
      case ex: Exception => println(sql)
        throw ex
    }
  }

  def execute(sql: String): Unit = {
    try {
      writer.execute(sql)
    } catch {
      case ex: Exception => println(sql)
        throw ex
    }
  }

  def getConnection(): Connection = {
    writer.getConnection
  }
}

object MysqlWriter {

  def apply(jdbc: String, username: String, password: String): MysqlWriter = {

    val f = () => {
      val conn = new Retryer().execute(DriverManager.getConnection(jdbc, username, password)).asInstanceOf[Connection]
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