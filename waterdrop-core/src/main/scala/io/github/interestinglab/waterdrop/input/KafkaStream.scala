package io.github.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStreamingInput
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._

class KafkaStream extends BaseStreamingInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  // kafka consumer configuration : http://kafka.apache.org/documentation.html#oldconsumerconfigs
  val consumerPrefix = "consumer"

  private var offsetRanges = Array[OffsetRange]()
  private var canCommitOffsets: CanCommitOffsets = _


  override def checkConfig(): (Boolean, String) = {

    config.hasPath("topics") match {
      case true => {
        val consumerConfig = config.getConfig(consumerPrefix)
        consumerConfig.hasPath("bootstrap.servers") &&
          !consumerConfig.getString("bootstrap.servers").trim.isEmpty &&
          consumerConfig.hasPath("group.id") &&
          !consumerConfig.getString("group.id").trim.isEmpty match {
          case true => (true, "")
          case false =>
            (false, "please specify [consumer.bootstrap.servers] and [consumer.group.id] as non-empty string")
        }
      }
      case false => (false, "please specify [topics] as non-empty string, multiple topics separated by \",\"")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "consumer.key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "consumer.value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    val consumerConfig = config.getConfig(consumerPrefix)
    val kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    println("[INFO] Input Kafka Params:")

    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

    val topics = config.getString("topics").split(",").toSet

    val inputDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    inputDStream.transform { rdd =>
      //get offset info
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      canCommitOffsets = inputDStream.asInstanceOf[CanCommitOffsets]
      rdd
    }.map(record => (record.key(), record.value()))
  }

  override def afterOutput: Unit = {
    //commit offset
    canCommitOffsets.commitAsync(offsetRanges)
  }

}
