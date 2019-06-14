package io.github.interestinglab.waterdrop.input.structuredstreaming

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingInput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.core.RowConstant
import io.github.interestinglab.waterdrop.utils.SparkSturctTypeUtil
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class KafkaStream extends BaseStructuredStreamingInput {

  var config: Config = ConfigFactory.empty()
  var schema = new StructType()
  var topics: String = _
  var consumer: KafkaConsumer[String, String] = _
  var kafkaParams: Map[String, String] = _
  val offsetMeta = new util.HashMap[String, util.HashMap[String, Long]]()
  val pollDuration = 100

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  // kafka consumer configuration : http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  val consumerPrefix = "consumer."

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("consumer.bootstrap.servers") && config.hasPath("topics") && config.hasPath("consumer.group.id") match {
      case true => {
        config.hasPath("offset.initial") match {
          case true => {
            List("begin", "end").contains(config.getString("offset.initial")) match {
              case true => (true, "")
              case false => (false, "error initial_offset please set value to begin or end")
            }
          }
          case false => (true, "")
        }

      }
      case false => (false, "please specify [consumer.bootstrap.servers] and [topics] and [consumer.group.id] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "offset.initial" -> "end",
        "maxOffsetsPerTrigger" -> 1000
      )
    )
    config = config.withFallback(defaultConfig)

    topics = config.getString("topics")

    config.hasPath("schema") match {
      case true => {
        val schemaJson = JSON.parseObject(config.getString("schema"))
        schema = SparkSturctTypeUtil.getStructType(schema, schemaJson)
      }
      case false => {}
    }

    //offset异步回写
    spark.streams.addListener(new StreamingQueryListener() {

      override def onQueryStarted(event: QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        //这个是kafkaInput的正则匹配
        val pattern = Pattern.compile(".+\\[Subscribe\\[(.+)\\]\\]")
        val sources = event.progress.sources
        sources.foreach(source => {
          val matcher = pattern.matcher(source.description)
          if (matcher.find() && topics.equals(matcher.group(1))) {
            //每个input负责监控自己的topic
            val endOffset = JSON.parseObject(source.endOffset)
            commitOffset(endOffset)
          }
        })
        consumer.commitSync()
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        consumer.close()
      }

      private def commitOffset(offsets:JSONObject):Unit = {
        offsets
          .keySet()
          .foreach(topic => {
            val partitionToOffset = offsets.getJSONObject(topic)
            partitionToOffset
              .keySet()
              .foreach(partition => {
                val offset = partitionToOffset.getLong(partition)
                val topicPartition = new TopicPartition(topic, Integer.parseInt(partition))
                consumer.poll(Duration.ofMillis(pollDuration).toMillis)
                consumer.seek(topicPartition, offset)
              })
          })
      }
    })

    val consumerConfig = TypesafeConfigUtils.extractSubConfig(config, consumerPrefix, false)
    kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    val topicList = initConsumer()

    kafkaParams = kafkaParams.-("group.id").map(kv=>("kafka." + kv._1,kv._2))

    //如果用户选择offset从broker获取
    if (config.hasPath("offset.location") && config.getString("offset.location").equals("broker")){
      setOffsetMeta(topicList)
      kafkaParams += ("startingOffsets" -> JSON.toJSONString(offsetMeta, SerializerFeature.WriteMapNullValue))
    }

    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    var dataFrame = spark.readStream
      .format("kafka")
      .option("subscribe", topics)
      .option("maxOffsetsPerTrigger",config.getInt("maxOffsetsPerTrigger"))
      .options(kafkaParams)
      .load()

    if (schema.size > 0) {
      var tmpDf = dataFrame.withColumn(RowConstant.TMP, from_json(col("value").cast(DataTypes.StringType), schema))
      schema.map { field =>
        tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
      }
      dataFrame = tmpDf.select(schema.fieldNames.map(col(_)): _*)
      if (config.hasPath("table_name")) {
        dataFrame.createOrReplaceTempView(config.getString("table_name"))
      }
    }else{
      dataFrame = dataFrame.withColumn(RowConstant.RAW_MESSAGE,col("value").cast(DataTypes.StringType))
        .select(RowConstant.RAW_MESSAGE)
    }
    dataFrame
  }

  private def initConsumer(): util.ArrayList[String] = {

    val props = new Properties() {{
        putAll(kafkaParams)
        put("topics", topics)
        put("enable.auto.commit", "false")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      }}

    println("[INFO] offset committer props:\n")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

    consumer = new KafkaConsumer[String, String](props)
    val topicList = new util.ArrayList[String]()
    topics.split(",").foreach(topicList.add(_))
    consumer.subscribe(topicList)

    topicList
  }

  private def setOffsetMeta(topicList: util.ArrayList[String]): Unit = {
    topicList.foreach(topic => {
      val partition2offset = new util.HashMap[String, Long]()
      val topicInfo = consumer.partitionsFor(topic)
      topicInfo.foreach(info => {
        val topicPartition = new TopicPartition(topic, info.partition())
        val metadata = consumer.committed(topicPartition)

        if (metadata == null) {
          val partitionBeginOffset = config.getString("offset.initial") match {
            case "end" => consumer.endOffsets(List(topicPartition))
            case "begin" => consumer.beginningOffsets(List(topicPartition))
          }
          partition2offset.put(String.valueOf(info.partition()), partitionBeginOffset.get(topicPartition))
        } else {
          partition2offset.put(String.valueOf(info.partition()), metadata.offset())
        }
      })
      offsetMeta.put(topic, partition2offset)
    })
  }
}