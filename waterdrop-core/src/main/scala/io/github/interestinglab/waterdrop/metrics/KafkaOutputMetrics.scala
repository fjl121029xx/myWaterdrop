package io.github.interestinglab.waterdrop.metrics

import io.github.interestinglab.waterdrop.output.{Kafka, KafkaSink}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.LongAccumulator

class KafkaOutputMetrics extends OutputMetrics
  with Serializable {

  private[this] var kafkaOutput: Kafka = _

  private[this] var topic: String = _
  private[this] var correct_accumulator: LongAccumulator = _
  private[this] var error_accumulator: LongAccumulator = _
  private[this] var sum_accumulator: LongAccumulator = _

  def this(kk: Kafka) {
    this
    this.kafkaOutput = kk

    this.topic = kafkaOutput.config.getString("topic")

    this.correct_accumulator = kafkaOutput.accumulators.get("kafka_" + topic + "_correct_accu")
    this.error_accumulator = kafkaOutput.accumulators.get("kafka_" + topic + "_error_accu")
    this.sum_accumulator = kafkaOutput.accumulators.get("kafka_" + topic + "_sum_accu")
  }

  def process(df: Dataset[Row]) {

    kafkaOutput.config.getString("serializer") match {
      case "text" => {
        df.foreachPartition(
          its => {
            while (its.hasNext) {
              val row = its.next()
              try {
                sum_accumulator.add(1L)
                kafkaOutput.kafkaSink.get.value
                  .send(kafkaOutput.config.getString("topic"), row.mkString)
                //                kafkaOutput.kafkaSink.get.value.send(kafkaOutput.config.getString("topic"), row.mkString)
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
        df.toJSON.foreachPartition(
          its => {
            while (its.hasNext) {
              val row = its.next()
              try {
                sum_accumulator.add(1L)
                kafkaOutput.kafkaSink.get.value
                  .send(kafkaOutput.config.getString("topic"), row)
                //                kafkaOutput.kafkaSink.get.value
                //                  .sendAndCallback(kafkaOutput.config.getString("topic"), row, correct_accumulator, error_accumulator, sum_accumulator)
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

