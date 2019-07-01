package io.github.interestinglab.waterdrop.utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

class MysqlWraper(createConn: () => Connection) extends Serializable {

  lazy val conn = createConn()

  def getConnection: Connection= conn

  def close(): Unit = {
    conn.close()
  }
}


object MysqlWraper {

  def apply(jdbc: String, username: String, password: String): MysqlWraper = {

    val f = () => {
      val props = new Properties(){{
        setProperty("user", username)
        setProperty("password", password)
        setProperty("autoReconnect", "true")
        setProperty("preferredTestQuery", "SELECT 1")
        setProperty("useServerPrepStmts", "false")
        setProperty("rewriteBatchedStatements", "true")
      }}

      println(s"[INFO] jdbc conf: $props")

      val conn = new Retryer().execute(DriverManager.getConnection(jdbc, props)).asInstanceOf[Connection]

      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new MysqlWraper(f)
  }

  def getJdbcConf(username: String, password: String): Properties = {

    new Properties() {{
        setProperty("user", username)
        setProperty("password", password)
        setProperty("autoReconnect", "true")
        setProperty("preferredTestQuery", "SELECT 1")
        setProperty("useServerPrepStmts", "false")
        setProperty("rewriteBatchedStatements", "true")
      }}
  }
}