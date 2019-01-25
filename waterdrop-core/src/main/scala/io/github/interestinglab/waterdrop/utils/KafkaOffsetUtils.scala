package io.github.interestinglab.waterdrop.utils

import java.sql.DriverManager
import java.{lang, util}

import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
 *
 * @author jiaquanyu
 *
 */
object KafkaInputOffsetUtils {

  def getOffset(
    mysqlConf: Config,
    topicPartitions: List[TopicPartition],
    appName: String,
    topics: String): util.HashMap[TopicPartition, lang.Long] = {

    val map = new util.HashMap[TopicPartition, lang.Long]()

    val conn = DriverManager.getConnection(
      mysqlConf.getString("jdbc"),
      mysqlConf.getString("username"),
      mysqlConf.getString("password"))
    val statement = conn.createStatement

    sys.addShutdownHook {
      conn.close()
      statement.close()
    }
    val sql = getQuerySql(topics, appName)

    println(sql)

    val rss = statement.executeQuery(sql)

    while (rss.next) {
      map.put(new TopicPartition(rss.getString("topic"), rss.getInt("partitionNum")), rss.getLong("untilOffset"))
    }
    map
  }

  def saveOffset(mysqlConf: Config, offsetRanges: Array[OffsetRange], appName: String, topics: String): Unit = {

    val conn = DriverManager.getConnection(
      mysqlConf.getString("jdbc"),
      mysqlConf.getString("username"),
      mysqlConf.getString("password"))

    val statement = conn.createStatement

    sys.addShutdownHook {
      conn.close()
      statement.close()
    }

    conn.setAutoCommit(false)

    val updateSql = getUpdateSql(topics, appName)
    val sql = getInsertSql(offsetRanges, appName)

    println(updateSql)
    println(sql)

    try {
      statement.execute(updateSql)
      statement.execute(sql)

      conn.commit()
    } catch {
      case ex: Exception =>
        conn.rollback()
        throw ex
    }

    conn.setAutoCommit(true)
  }

  val getQuerySql = (topics: String, appName: String) =>
    "SELECT topic,partitionNum,untilOffset FROM tbl_wd_kafka_offset where topic in (\"" +
      topics.replace(",", "\",\"") + "\") AND current = 1 AND appName = \"" + appName + "\""

  val getUpdateSql = (topics: String, appName: String) =>
    "UPDATE tbl_wd_kafka_offset SET current = 0 WHERE topic in (\"" +
      topics.replace(",", "\",\"") + "\") AND current = 1 AND appName = \"" + appName + "\""

  val getInsertSql = (offsetRanges: Array[OffsetRange], appName: String) => {

    val sqlPrefix =
      "INSERT INTO tbl_wd_kafka_offset (topic,partitionNum,appName,current,fromOffset,untilOffset) VALUES "

    val sb = new StringBuilder
    offsetRanges.foreach(or => {
      sb.append(
        "(\"" + or.topic + "\"," + or.partition + ",\"" + appName + "\",1," + or.fromOffset + "," + or.untilOffset + "),")
    })

    sqlPrefix + sb.toString.substring(0, sb.length - 1)
  }

}
