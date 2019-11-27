package io.github.interestinglab.waterdrop.output

import java.sql.{PreparedStatement, Types}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import oracle.jdbc.OracleConnection
import oracle.jdbc.pool.OracleDataSource
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

class Oracle18c extends BaseOutput {

  val config_param: Seq[String] = Seq("url", "user", "password", "table")

  var config: Config = ConfigFactory.empty()

  var columns: List[String] = List.empty
  var insert_sql: String = _

  override def getConfig(): Config = this.config

  override def setConfig(config: Config): Unit = this.config = config

  override def checkConfig(): (Boolean, String) = {

    val ps = config_param.foldLeft(0)((b, a) => if (config.getIsNull(a)) b else b + 1)

    ps == config_param.size match {
      case true => (true, "")
      case false => (false, "both url and user and password and table are required")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    //获取 oracle 表字段
    columns = getColumns //生成预编译sql
    insert_sql = getInsertSql(columns)

  }

  override def process(df: Dataset[Row]): Unit = {

    val sc = df.sparkSession.sparkContext

    val bc_url = sc.broadcast(config.getString("url"))
    val bc_user = sc.broadcast(config.getString("user"))
    val bc_password = sc.broadcast(config.getString("password"))
    val bc_table = sc.broadcast(config.getString("table"))
    val bc_insert_sql = sc.broadcast(insert_sql)
    val bc_columns = sc.broadcast(columns)

    df.foreachPartition(rowIterator => {
      val conn = getConnection(bc_url.value, bc_user.value, bc_password.value)
      val ps = conn.prepareStatement(bc_insert_sql.value)
      iterProcess(rowIterator, bc_columns.value.toArray, ps)

    })
  }

  /**
   * 获取表字段
   */
  def getColumns(): List[String] = {

    val query_columns_sql = s"select COLUMN_NAME from user_tab_columns where table_name = '${config.getString("table").toUpperCase}'"

    val conn = getConnection(config.getString("url"), config.getString("user"), config.getString("password"))
    val statement = conn.createStatement()

    val rs = statement.executeQuery(query_columns_sql)

    val lb = new ListBuffer[String]


    lb.zipWithIndex.foreach(println)
    while (rs.next) {
      lb.append(rs.getString(1))
    }

    statement.close()
    conn.close()

    lb.toList
  }

  def getInsertSql(columns: List[String]): String = {

    val fieldStr = columns.mkString("(", ",", ")")

    val sb = new StringBuffer()
    for (_ <- columns.toIndexedSeq) yield sb.append("?")
    val valueStr = sb.toString.mkString("(", ",", ")")

    s"INSERT INTO ${config.getString("table")}$fieldStr VALUES$valueStr"
  }

  /**
   * 获取oracle连接
   */
  def getConnection(oracleurl: String, user: String, password: String): OracleConnection = {

    val info = new Properties {
      {
        put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, user)
        put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, password)
        put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20")
      }
    }

    val ods = new OracleDataSource {
      {
        setURL(oracleurl)
        setConnectionProperties(info)
      }
    }

    ods.getConnection.asInstanceOf[OracleConnection]
  }

  private def iterProcess(it: Iterator[Row], cols: Array[String], ps: PreparedStatement): (Int, Int) = {

    var i = 0
    var insertSum = 0
    var errSum = 0

    while (it.hasNext) {
      val row = it.next
      setPrepareStatement(cols, row, ps)
      ps.addBatch()
      i += 1

      if (i == config.getInt("batch.count") || (!it.hasNext)) {
        try {
          ps.executeBatch

        } catch {
          case e: Exception =>
            e.printStackTrace()
        }

        ps.clearBatch
        i = 0
      }
    }
    (insertSum, errSum)
  }

  private def setPrepareStatement(fields: Array[String], row: Row, ps: PreparedStatement): Unit = {
    var p = 1
    val zColumns = columns.zipWithIndex

    for ((cname: String, v: Int) <- zColumns) {
      val f = row.getAs[Any](cname).toString
      ps.setString(v + 1, f)
      p += 1
    }
  }
}