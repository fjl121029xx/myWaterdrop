package io.github.interestinglab.waterdrop.utils

import java.{lang, util}
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.mutable.ListBuffer

import scala.collection.JavaConversions._

/**
 * @author jiaquanyu
 */
class KafkaSource(createConsumer: () => KafkaConsumer[String, String]) extends Serializable {

  lazy val consumer = createConsumer()

  def getTopicPartition(topic: String): List[TopicPartition] = {
    val list = new ListBuffer[TopicPartition]
    consumer
      .partitionsFor(topic)
      .foreach(partitionInfo => {
        list.append(new TopicPartition(topic, partitionInfo.partition))
      })
    list.toList
  }

  def getBeginningOffset(topicPartitions: List[TopicPartition]): util.Map[TopicPartition, lang.Long] = {
    consumer.beginningOffsets(topicPartitions)
  }

  def getTimeOffset(
    topicPartitions: List[TopicPartition],
    offsetTimeStamp: Long): util.Map[TopicPartition, lang.Long] = {

    val topicPartitionWithTimeStamp = topicPartitions.map(t => t -> lang.Long.valueOf(offsetTimeStamp)).toMap[TopicPartition,lang.Long]
    println("[INFO] topicPartitionWithTimeStamp: " + topicPartitionWithTimeStamp)

    val partitionToTimestamp =
      consumer.offsetsForTimes(topicPartitionWithTimeStamp)
    println("[INFO] partitionToTimestamp: " + partitionToTimestamp)
    partitionToTimestamp.map(t => {
      (t._1, if(t._2 == null) null else lang.Long.valueOf(t._2.offset))
    })
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
