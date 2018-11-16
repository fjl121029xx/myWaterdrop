package io.github.interestinglab.waterdrop.input

import java.{lang, util}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class Kafka extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  var kafkaSource: Option[Broadcast[KafkaSource]] = None

  val consumerPrefix = "consumer"

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
        consumerConfig.hasPath("zookeeper.connect") &&
          !consumerConfig.getString("zookeeper.connect").trim.isEmpty &&
          consumerConfig.hasPath("group.id") &&
          !consumerConfig.getString("group.id").trim.isEmpty match {
          case true => (true, "")
          case false =>
            (false, "please specify [consumer.zookeeper.connect] and [consumer.group.id] as non-empty string")
        }
      }
      case false => (false, "please specify [topics] as non-empty string, multiple topics separated by \",\"")
    }
  }


  override def getDataset(sparkSession: SparkSession): Dataset[Row] = {


    val consumerConfig = config.getConfig(consumerPrefix)

    consumerConfig.entrySet()

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

    //get offsets range
    val offsetRanges: Array[OffsetRange] = topics.flatMap(topic => {
      val list = new ListBuffer[OffsetRange]
      val tps = kafkaSource.get.value.getTopicPartition(topic)
      val endOffsets = kafkaSource.get.value.getEndOffset(tps)
      //TODO 判断是否有上次任务保存offset 如不包含 从topic start offset开始
      val beginOffset = kafkaSource.get.value.getBeginningOffset(tps)

      tps.foreach(tp => {
        list.append(OffsetRange(tp, beginOffset.get(tp), endOffsets.get(tp)))
      })
      list.toList
    }).toArray

    //start offset end offset(latest offset)
    val rdd = KafkaUtils.createRDD[String, String](sparkSession.sparkContext, kafkaParams, offsetRanges, PreferConsistent).map(cr => {
      //string to Row
      RowFactory.create(cr.value)
    })

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    //RDD convert DateSet[Row]
    sparkSession.createDataset(rdd)(RowEncoder(schema))
  }

  def saveOffset(topicOffsets: RDD[String]): Unit = {
    //TODO 保存本次任务执行完毕end offset
    topicOffsets.saveAsTextFile("")
  }

  def getLastOffset(sparkSession: SparkSession): Unit = {
  //TODO 获取上次任务end offset 作为本次start offset
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
