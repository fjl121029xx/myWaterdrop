package io.github.interestinglab.waterdrop.metrics

import com.typesafe.config.Config
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source2.HllStatBatchErrorSource

class OutputMetrics {

}

object OutputMetrics {

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