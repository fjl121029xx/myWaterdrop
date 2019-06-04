package io.github.interestinglab.waterdrop.utils

import java.sql.{Connection, DriverManager}

class MysqlWraper(createConn: () => Connection) extends Serializable {

  val conn = createConn()

  def getConnection: Connection= conn

  def close(): Unit = {
    conn.close()
  }
}


object MysqlWraper {

  def apply(jdbc: String, username: String, password: String): MysqlWraper = {

    val f = () => {
      val conn = new Retryer().execute(DriverManager.getConnection(jdbc, username, password)).asInstanceOf[Connection]

      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new MysqlWraper(f)
  }
}