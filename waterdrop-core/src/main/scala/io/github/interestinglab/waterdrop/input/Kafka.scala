package io.github.interestinglab.waterdrop.input

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.utils.KafkaSource
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._

class Kafka extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  var kafkaSource: Broadcast[KafkaSource] = _
  var offsetRanges: Array[OffsetRange] = _

  val consumerPrefix = "consumer"
  val offsetStartTimeStamp = "offset.start.timestamp"
  val offsetEndTimeStamp = "offset.end.timestamp"

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
        consumerConfig.hasPath("bootstrap.servers") && !consumerConfig.getString("bootstrap.servers").trim.isEmpty &&
          config.hasPath(offsetStartTimeStamp) && config.hasPath(offsetEndTimeStamp)  match {
          case true => (true, "")
          case false =>
            (false, "[consumer.bootstrap.servers],[offset.start.timestamp] and [offset.end.timestamp] are required!!! " +
              "then [consumer.bootstrap.servers] as non-empty string")
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

    kafkaSource = sparkSession.sparkContext.broadcast(KafkaSource(props))

    val topicPartitions = config.getString("topics").split(",").toList.flatMap(kafkaSource.value.getTopicPartition(_))

    var startOffsets = kafkaSource.value.getTimeOffset(topicPartitions,config.getLong(offsetStartTimeStamp))

    //topic partition 全部无数据
    if (startOffsets.count(_._2 == null) == startOffsets.size) {
      println("[INFO] topics no data")
      sparkSession.emptyDataFrame
    } else {
      startOffsets = startOffsets.filter(_._2 != null)
      val endOffsets = kafkaSource.value.getTimeOffset(startOffsets.keys.toList, config.getLong(offsetEndTimeStamp))
      kafkaSource.value.getEndOffset(endOffsets.filter(_._2 == null).keys.toList).foldLeft(endOffsets)((endOffsets, tp) => {
        endOffsets.put(tp._1, tp._2)
        endOffsets
      })

      offsetRanges = startOffsets.keys.map(tp => OffsetRange(tp, startOffsets.get(tp), endOffsets.get(tp))).toArray

      showOffsetInfo(offsetRanges)

      val rdd = KafkaUtils.createRDD[String, String](sparkSession.sparkContext, kafkaParams, offsetRanges, PreferConsistent).map(cr => {
        //string to Row
        RowFactory.create(cr.value)
      })

      val schema = new StructType().add("raw_message", DataTypes.StringType)

      //RDD convert DateSet[Row]
      sparkSession.createDataset(rdd)(RowEncoder(schema))
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
