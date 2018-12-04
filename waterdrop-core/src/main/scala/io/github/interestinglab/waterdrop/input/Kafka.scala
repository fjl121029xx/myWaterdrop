package io.github.interestinglab.waterdrop.input

import java.sql.{Connection, DriverManager}
import java.{lang, util}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class Kafka extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  var kafkaSource: Option[Broadcast[KafkaSource]] = None
  var offsetRanges: Option[Array[OffsetRange]] = None

  val consumerPrefix = "consumer"
  val mysqlPrefix = "mysql"

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    return this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("topics") match {
      case true => {
        val consumerConfig = config.getConfig(consumerPrefix)
        consumerConfig.hasPath("bootstrap.servers") &&
          !consumerConfig.getString("bootstrap.servers").trim.isEmpty  match {
          case true => (true, "")
          case false =>
            (false, "please specify [consumer.bootstrap.servers] as non-empty string")
        }
      }
      case false => (false, "please specify [topics] as non-empty string, multiple topics separated by \",\"")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "consumer.auto.offset.reset" -> "earliest", //默认auto.offset.reset=earliest
        "consumer.key.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer",
        "consumer.value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer"
      )
    )
    config = config.withFallback(defaultConfig)
  }


  override def getDataset(sparkSession: SparkSession): Dataset[Row] = {


    val consumerConfig = config.getConfig(consumerPrefix)

    val kafkaParams = mapAsJavaMap[String, Object](consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      }))

    println("[INFO] Input Kafka Params:")

    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

    val topics = config.getString("topics").split(",").toSet

    //kafka consumer properties
    val props = new Properties()
    config
      .getConfig(consumerPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        props.put(key, value)
      })

    kafkaSource = Some(sparkSession.sparkContext.broadcast(KafkaSource(props)))

    val beginOffset = getBeginningOffsets(topics)
    //get offsets range
    offsetRanges = Some(topics.flatMap(topic => {
      val list = new ListBuffer[OffsetRange]
      val tps = kafkaSource.get.value.getTopicPartition(topic)
      val endOffsets = kafkaSource.get.value.getEndOffset(tps)

      tps.foreach(tp => {
        list.append(OffsetRange(tp, beginOffset.get(tp), endOffsets.get(tp)))
      })
      list.toList
    }).toArray)

    //print offset info and sum
    showOffsetInfo(offsetRanges.get)

    //start offset end offset(latest offset)
    val rdd = KafkaUtils.createRDD[String, String](sparkSession.sparkContext, kafkaParams, offsetRanges.get, PreferConsistent).map(cr => {
      //string to Row
      RowFactory.create(cr.value)
    })

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    //RDD convert DateSet[Row]
    sparkSession.createDataset(rdd)(RowEncoder(schema))
  }

  override def afterBatch: Unit = {

    //save offset
    config.hasPath(mysqlPrefix) match {
      case true => saveOffset(offsetRanges.get)
      case false => //do nothing
    }
  }

  def saveOffset(offsetRanges:Array[OffsetRange]): Unit = {
    val mysqlConf = config.getConfig(mysqlPrefix)
    var conn: Connection = null

    conn = DriverManager.getConnection(mysqlConf.getString("jdbc"), mysqlConf.getString("username"), mysqlConf.getString("password"))
    val statement = conn.createStatement

    sys.addShutdownHook {
      conn.close()
      statement.close()
    }

    val updateSql = "UPDATE tbl_wd_kafka_offset SET current = 0 WHERE topic in (\"" + config.getString("topics").replace(",","\",\"") + "\") AND current = 1"

    val insertSql = "INSERT INTO tbl_wd_kafka_offset (topic,partitionNum,current,fromOffset,untilOffset) VALUES "

    val sb = new StringBuilder
    offsetRanges.foreach(or => {
      sb.append("(\"" + or.topic + "\"," + or.partition + ",1," + or.fromOffset + "," + or.untilOffset + "),")
    })

    val sql = insertSql + sb.toString.substring(0, sb.length - 1)

    println(updateSql)
    println(sql)

    statement.executeUpdate(updateSql)
    statement.execute(sql)
  }

  def getTopicPartitions(topics: Set[String]): List[TopicPartition] = topics.flatMap(kafkaSource.get.value.getTopicPartition(_)).toList


  def getBeginningOffsets(topics: Set[String]): util.Map[TopicPartition, lang.Long] = {

    val topicPartitions = getTopicPartitions(topics)

    def getKafkaSourceOffset(topicPartitions: List[TopicPartition]) = kafkaSource.get.value.getBeginningOffset(topicPartitions)

    //判断是否包含mysql配置
    config.hasPath(mysqlPrefix) match {
      case true => {

        val mysqlConf = config.getConfig(mysqlPrefix)
        var conn: Connection = null

        conn = DriverManager.getConnection(mysqlConf.getString("jdbc"), mysqlConf.getString("username"), mysqlConf.getString("password"))
        val statement = conn.createStatement

        sys.addShutdownHook {
          conn.close()
          statement.close()
        }
        // scalastyle:off
        val sql = "SELECT topic,partitionNum,untilOffset FROM tbl_wd_kafka_offset where topic in (\"" + config.getString("topics").replace(",","\",\"") + "\") AND current = 1"
        // scalastyle:on
        println(sql)

        Try(statement.executeQuery(sql)) match {
          case Success(rss) => {

            var tps = topicPartitions.to[ListBuffer]

            val map = new util.HashMap[TopicPartition, lang.Long]()
            while (rss.next) {
              map.put(new TopicPartition(rss.getString("topic"), rss.getInt("partitionNum")), rss.getLong("untilOffset"))
            }
            map.entrySet().foreach(entry => {
              tps = tps.-(entry.getKey)
            })
            map.putAll(getKafkaSourceOffset(tps.toList))

            map
          }
          case Failure(rsf)
          => {
            throw new Exception(rsf.getMessage)
          }
        }
      }
      case false => {
        getKafkaSourceOffset(topicPartitions)
      }
    }
  }

  def showOffsetInfo(offsetRanges: Array[OffsetRange]): Unit = {
    var sum = 0L
    println("[INFO] Kafka Topic Offset:")
    offsetRanges.foreach(offsetRange => {
      println(offsetRange)
      sum += offsetRange.untilOffset - offsetRange.fromOffset
    })
    println("[INFO] Kafka Range Sum :" + sum)
  }
}


class KafkaSource(createConsumer: () => KafkaConsumer[String, String]) extends Serializable {

  lazy val consumer = createConsumer()

  def getTopicPartition(topic: String): List[TopicPartition] = {
    var list = new ListBuffer[TopicPartition]
    consumer.partitionsFor(topic).foreach(partitionInfo => {
      list.append(new TopicPartition(topic, partitionInfo.partition()))
    })
    list.toList
  }

  def getBeginningOffset(topicPartitions: List[TopicPartition]): util.Map[TopicPartition, lang.Long] = {
    consumer.beginningOffsets(topicPartitions)

  }

  def getEndOffset(topicPartitions: List[TopicPartition]): util.Map[TopicPartition, lang.Long] = {
    consumer.endOffsets(topicPartitions)

  }
}

object KafkaSource {
  def apply(config: Properties): KafkaSource = {
    val f = () => {
      val consumer = new KafkaConsumer[String, String](config)

      sys.addShutdownHook {
        consumer.close()
      }

      consumer
    }
    new KafkaSource(f)
  }

}
