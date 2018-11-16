package io.github.interestinglab.waterdrop.input

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStreamingInput
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class SocketStream extends BaseStreamingInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "host" -> "localhost",
        "port" -> 9999
      ))
    config = config.withFallback(defaultConfig)
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    ssc
      .socketTextStream(config.getString("host"), config.getInt("port"))
      .map(s => {
        ("", s)
      })
  }
}
