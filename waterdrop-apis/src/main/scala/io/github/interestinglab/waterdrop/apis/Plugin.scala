package io.github.interestinglab.waterdrop.apis

import java.util

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
  * checkConfig --> prepare
  */
trait Plugin extends Serializable with Logging {

  /**
    * Set Config.
    * */
  def setConfig(config: Config): Unit

  /**
    * Get Config.
    * */
  def getConfig(): Config

  /**
    *  Return true and empty string if config is valid, return false and error message if config is invalid.
    */
  def checkConfig(): (Boolean, String)

  /**
    * Get Plugin Name.
    */
  def name: String = this.getClass.getName

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  def prepare(spark: SparkSession): Unit = {}

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   * correct : streaming-metrics accumulator
   * error : streaming-metrics accumulator
   * sum : streaming-metrics accumulator
   */
  def prepareWithMetrics(spark: SparkSession, accu_map: util.HashMap[String, LongAccumulator]): Unit = {}
}