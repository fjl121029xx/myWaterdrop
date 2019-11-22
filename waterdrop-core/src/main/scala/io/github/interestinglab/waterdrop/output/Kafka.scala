package io.github.interestinglab.waterdrop.output

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConversions._

class Kafka extends BaseOutput {

  val producerPrefix = "producer"

  var kafkaSink: Option[Broadcast[KafkaSink]] = None

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   **/
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   **/
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val producerConfig = config.getConfig(producerPrefix)

    config.hasPath("topic") && producerConfig.hasPath("bootstrap.servers") match {
      case true => (true, "")
      case false => (false, "please specify [topic] and [producer.bootstrap.servers]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "serializer" -> "json", //text json
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )

    config = config.withFallback(defaultConfig)

    val props = new Properties()
    config
      .getConfig(producerPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        props.put(key, value)
      })

    println("[INFO] Kafka Output properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    })

    kafkaSink = Some(spark.sparkContext.broadcast(KafkaSink(props)))
  }

  override def prepareWithMetrics(spark: SparkSession, accu_map: util.HashMap[String, LongAccumulator]): Unit = {
    super.prepareWithMetrics(spark, accu_map)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "serializer" -> "json", //text json
        producerPrefix + ".key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        producerPrefix + ".value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )
    )

    config = config.withFallback(defaultConfig)

    val props = new Properties()
    config
      .getConfig(producerPrefix)
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        props.put(key, value)
      })

    println("[INFO] Kafka Output properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    })

    kafkaSink = Some(spark.sparkContext.broadcast(KafkaSink(props)))
  }

  override def process(df: Dataset[Row]) {

    config.getString("serializer") match {
      case "text" => {
        df.foreach { row =>

          kafkaSink.get.value.send(config.getString("topic"), row.mkString)
        }
      }
      case _ => {
        df.toJSON.foreach(row => {
          kafkaSink.get.value.send(config.getString("topic"), row)
        })
      }
    }

  }

  override def processWithMetrics(df: Dataset[Row], accu_map: util.HashMap[String, LongAccumulator]) {

    config.getString("serializer") match {
      case "text" => {
        //        df.foreach { row =>
        //
        //          kafkaSink.get.value.send(config.getString("topic"), row.mkString)
        //        }
        df.foreachPartition(
          its => {
            val correct_accumulator = accu_map.get(this.getClass.getSimpleName + "_correct_accu")
            val error_accumulator = accu_map.get(this.getClass.getSimpleName + "_error_accu")
            val sum_accumulator = accu_map.get(this.getClass.getSimpleName + "_sum_accu")
            while (its.hasNext) {
              val row = its.next()
              sum_accumulator.add(1)
              try {
                kafkaSink.get.value.send(config.getString("topic"), row.mkString)
                correct_accumulator.add(1L)
              } catch {
                case ex: Exception =>
                  error_accumulator.add(1L)
                  ex.printStackTrace()
              }

            }
          }
        )
      }
      case _ => {
        //        df.toJSON.foreach(row => {
        //          kafkaSink.get.value.send(config.getString("topic"), row)
        //        })
        df.toJSON.foreachPartition(
          its => {
            val correct_accumulator = accu_map.get(this.getClass.getSimpleName + "_correct_accu")
            val error_accumulator = accu_map.get(this.getClass.getSimpleName + "_error_accu")
            val sum_accumulator = accu_map.get(this.getClass.getSimpleName + "_sum_accu")
            while (its.hasNext) {
              val row = its.next()
              sum_accumulator.add(1)
              try {
                kafkaSink.get.value.send(config.getString("topic"), row)
                correct_accumulator.add(1L)
              } catch {
                case ex: Exception =>
                  error_accumulator.add(1L)
                  ex.printStackTrace()
              }
            }

          }
        )
      }
    }

  }

}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit =
    producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}
