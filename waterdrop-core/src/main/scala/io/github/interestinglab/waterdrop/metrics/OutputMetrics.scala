package io.github.interestinglab.waterdrop.metrics

import java.util

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.{BaseOutput, Plugin}
import io.github.interestinglab.waterdrop.output.{Kafka, Mysql}
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source2.HllStatBatchErrorSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.LongAccumulator

abstract class OutputMetrics {

  def checkHasAccumulator(): Boolean = false

}

object OutputMetrics {

  /**
   * 注册 output metrics
   */
  def registeMetrics(ssc: StreamingContext, outputs: List[BaseOutput]): util.HashMap[String, LongAccumulator] = {
    val ms = SparkEnv.get.metricsSystem
    val outputNames = outputs.map(f => f.getClass.getSimpleName.toLowerCase() + "_" + OutputMetrics.getOutputAlias(f.name, f.getConfig()))
    val mySource = new HllStatBatchErrorSource(ssc, outputNames)
    ms.registerSource(mySource)
    val accumulators: util.HashMap[String, LongAccumulator] = mySource.getAllAccu()
    accumulators
  }

  /**
   * output 配置 accumulators
   */
  def basePrepareWithMetrics(sparkSession: SparkSession,
                             accumulators: util.HashMap[String, LongAccumulator], plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
        if (p.isInstanceOf[Mysql] || p.isInstanceOf[Kafka]) {
          p.setAccuMap(accumulators)
        }
      }
    }
  }

  /**
   * 获取output alias
   */
  def getOutputAlias(output: String, config: Config): String = {
    var name = "none"
    if (output.equalsIgnoreCase("io.github.interestinglab.waterdrop.output.Mysql")) {
      name = config.getString("table")
    } else if (output.equalsIgnoreCase("io.github.interestinglab.waterdrop.output.Kafka")) {
      name = config.getString("topic")
    }
    name
  }


}