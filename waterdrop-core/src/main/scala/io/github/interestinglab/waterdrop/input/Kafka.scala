package io.github.interestinglab.waterdrop.input

import java.time.{LocalDateTime, ZoneId}
import java.util.Properties
import java.{lang, util}

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.utils.{KafkaInputOffsetUtils, KafkaSource, Retryer}
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
  var mysqlConf: Config = ConfigFactory.empty()

  var kafkaSource: Option[Broadcast[KafkaSource]] = None
  var offsetRanges: Option[Array[OffsetRange]] = None

  val consumerPrefix = "consumer"
  val mysqlPrefix = "mysql"

  val MAX_RETRY_COUNT = 10

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("topics") match {
      case true => {
        val consumerConfig = config.getConfig(consumerPrefix)
        consumerConfig.hasPath("bootstrap.servers") &&
          !consumerConfig.getString("bootstrap.servers").trim.isEmpty match {
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
        "consumer.key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "consumer.value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def getDataset(sparkSession: SparkSession): Dataset[Row] = {

    val consumerConfig = config.getConfig(consumerPrefix)

    val kafkaParams = mapAsJavaMap[String, Object](
      consumerConfig
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

    val beginOffset = getBeginningOffsets(topics, sparkSession.sparkContext.appName)
    //get offsets range
    offsetRanges = Some(
      topics
        .flatMap(topic => {
          val list = new ListBuffer[OffsetRange]
          val tps = kafkaSource.get.value.getTopicPartition(topic)
          val endOffsets = kafkaSource.get.value.getEndOffset(tps)

          tps.foreach(tp => {
            list.append(OffsetRange(tp, beginOffset.get(tp), endOffsets.get(tp)))
          })
          list.toList
        })
        .toArray)

    //print offset info and sum
    showOffsetInfo(offsetRanges.get)

    //start offset end offset(latest offset)
    val rdd = KafkaUtils
      .createRDD[String, String](sparkSession.sparkContext, kafkaParams, offsetRanges.get, PreferConsistent)
      .map(cr => {
        //string to Row
        RowFactory.create(cr.value)
      })

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    //RDD convert DateSet[Row]
    sparkSession.createDataset(rdd)(RowEncoder(schema))
  }

  override def afterBatch(spark: SparkSession): Unit = {

    //save offset
    config.hasPath(mysqlPrefix) match {
      case true =>
        Try(
          new Retryer().execute(KafkaInputOffsetUtils
            .saveOffset(mysqlConf, offsetRanges.get, spark.sparkContext.appName, config.getString("topics")))) match {
          case Success(i) => println("[INFO] save offset done!")
          case Failure(f) => println("[ERROR] save offset error: " + f.getMessage)
        }
      case false => //do nothing
    }
  }

  def getBeginningOffsets(topics: Set[String], appName: String): util.Map[TopicPartition, lang.Long] = {

    val topicPartitions = getTopicPartitions(topics)

    //判断是否包含mysql配置
    config.hasPath(mysqlPrefix) match {
      case true => {

        mysqlConf = config.getConfig("mysql")

        var tps = topicPartitions.to[ListBuffer]

        val map = Try(
          new Retryer().execute(KafkaInputOffsetUtils
            .getOffset(mysqlConf, topicPartitions, appName, config.getString("topics")))) match {
          case Success(map) => map.asInstanceOf[util.HashMap[TopicPartition, lang.Long]]
//          case Failure(f) => new util.HashMap[TopicPartition, lang.Long]()
          case Failure(f) => new util.HashMap[TopicPartition, lang.Long](kafkaSource.get.value.getTimeOffset(tps.toList, getHourAgoTimeStamp(2)))
        }

        map
          .entrySet()
          .foreach(entry => {
            tps = tps.-(entry.getKey)
          })
        //如mysql不包含的topic-partition信息或获取offset元数据失败 默认获取2小时前的offset
        if (tps.length > 0) {
//          map.putAll(kafkaSource.get.value.getTimeOffset(tps.toList, getHourAgoTimeStamp(2)))
          map.putAll(kafkaSource.get.value.getBeginningOffset(topicPartitions))
        }

        map
      }
      case false => {
        kafkaSource.get.value.getBeginningOffset(topicPartitions)
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

  val getTopicPartitions = (topics: Set[String]) => topics.flatMap(kafkaSource.get.value.getTopicPartition(_)).toList

  val getHourAgoTimeStamp = (hour: Int) => {
    val zone = ZoneId.systemDefault()
    val localDateTime = LocalDateTime.now().minusHours(hour)
    localDateTime.atZone(zone).toInstant.toEpochMilli
  }

}
