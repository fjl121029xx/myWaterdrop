package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.utils.AsciiArt
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.protocol.HTTP
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Waterdrop extends Logging {

  var inputRecords = 0L
  var inputBytes = 0L
  var outputWritten = 0L
  var outputBytes = 0L

  def main(args: Array[String]) {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)

        val configFilePath = Common.getDeployMode match {
          case Some(m) => {
            if (m.equals("cluster")) {
              // only keep filename in cluster mode
              new Path(cmdArgs.configFile).getName
            } else {
              cmdArgs.configFile
            }
          }
        }

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {

            Try(entrypoint(configFilePath)) match {
              case Success(_) => {}
              case Failure(exception) => {
                exception.isInstanceOf[ConfigRuntimeException] match {
                  case true => {
                    showConfigError(exception)
                  }
                  case false => {
                    showFatalError(exception)
                  }
                }
              }
            }
          }
        }
      }
      case None =>
      // CommandLineUtils.parser.showUsageAsError()
      // CommandLineUtils.parser.terminate(Right(()))
    }
  }

  private[waterdrop] def getConfigFilePath(cmdArgs: CommandLineArgs): String = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {
          // only keep filename in cluster mode
          new Path(cmdArgs.configFile).getName
        } else {
          cmdArgs.configFile
        }
      }
    }
  }

  private[waterdrop] def showWaterdropAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Waterdrop")
  }

  private[waterdrop] def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
  }

  private[waterdrop] def showFatalError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Fatal Error, \n")
    println(
      "Please contact garygaowork@gmail.com or issue a bug in https://github.com/InterestingLab/waterdrop/issues\n")
    println("Reason: " + errorMsg + "\n")
    println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable))
    println("\n===============================================================================\n\n\n")
  }

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    val staticInputs = configBuilder.createStaticInputs
    val streamingInputs = configBuilder.createStreamingInputs
    val outputs = configBuilder.createOutputs
    val filters = configBuilder.createFilters


    baseCheckConfig(staticInputs,streamingInputs,outputs,filters)

    process(configBuilder, staticInputs, streamingInputs, filters, outputs)
  }

  private def process(
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    streamingInputs: List[BaseStreamingInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    streamingInputs.size match {
      case 0 => {

        sparkSession.sparkContext.addSparkListener(new SparkListener() {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
            val metrics = taskEnd.taskMetrics
            if (metrics.inputMetrics != None) {
              println("[DEBUG] metrics input: " + metrics.inputMetrics.recordsRead)
              inputRecords += metrics.inputMetrics.recordsRead
              inputBytes += metrics.inputMetrics.bytesRead
            }
            if (metrics.outputMetrics != None) {
              println("[DEBUG] metrics output: " + metrics.outputMetrics.recordsWritten)
              outputWritten += metrics.outputMetrics.recordsWritten
              outputBytes += metrics.outputMetrics.bytesWritten
            }
          }
        })

        batchProcessing(sparkSession, configBuilder, staticInputs, filters, outputs)

        //write metrics to influxdb
        write2Influx(sparkSession.sparkContext, inputRecords, inputBytes, outputWritten, outputBytes)

        println("[INFO] input record size: " + inputRecords)
        println("[INFO] input byte size: " + inputBytes)
        println("[INFO] output write size: " + outputWritten)
        println("[INFO] output byte size: " + outputBytes)
      }
      case _ => {
        streamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, filters, outputs)
      }
    }
  }

  /**
   * Streaming Processing
   **/
  private def streamingProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    streamingInputs: List[BaseStreamingInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    val sparkConfig = configBuilder.getSparkConfigs
    val duration = sparkConfig.getLong("spark.streaming.batchDuration")
//    val sparkConf = createSparkConf(configBuilder)
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))

    basePrepare(sparkSession, staticInputs, streamingInputs, filters, outputs)

    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    import java.lang.management.ManagementFactory
    val inputArgs = ManagementFactory.getRuntimeMXBean.getInputArguments

    logInfo("[JVM options]" + inputArgs.toString)

    val dstreamList = streamingInputs.map(p => {
      p.getDStream(ssc)
    })

    val unionedDStream = dstreamList.reduce((d1, d2) => {
      d1.union(d2)
    })

    val dStream = unionedDStream.mapPartitions { partitions =>
      val strIterator = partitions.map(r => r._2)
      val strList = strIterator.toList
      strList.iterator
    }

    dStream.foreachRDD { strRDD =>
      val rowsRDD = strRDD.mapPartitions { partitions =>
        val row = partitions.map(Row(_))
        val rows = row.toList
        rows.iterator
      }

      // For implicit conversions like converting RDDs to DataFrames

      val schema = new StructType().add("raw_message", DataTypes.StringType)
      val encoder = RowEncoder(schema)
      var ds = sparkSession.createDataset(rowsRDD)(encoder)

      // Ignore empty schema dataset
      if (ds.columns.length > 0 && !rowsRDD.isEmpty()) {

        for (f <- filters) {
          ds = f.process(sparkSession, ds)
        }

        streamingInputs.foreach(p => {
          p.beforeOutput
        })

        try {
          outputs.foreach(p => {
            p.process(ds)
          })
        } catch {
          case ex: Exception => throw ex
        }

        streamingInputs.foreach(p => {
          p.afterOutput
        })
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Batch Processing
    **/
  private def batchProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    basePrepare(sparkSession, staticInputs, filters, outputs)

    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    if (staticInputs.nonEmpty) {
      for (input <- staticInputs) {
        var ds = input.getDataset(sparkSession)
        if (ds.columns.length > 0) {
          for (f <- filters) {
            ds = f.process(sparkSession, ds)
          }
          outputs.foreach(p => {
            p.process(ds)
          })
        }
        input.afterBatch(sparkSession)
      }
    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }

  }

  private[waterdrop] def basePrepare(sparkSession: SparkSession, plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
      }
    }
  }

  private[waterdrop] def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }

  private[waterdrop] def baseCheckConfig(plugins: List[Plugin]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        val (isValid, msg) = Try(p.checkConfig) match {
          case Success(info) => {
            val (ret, message) = info
            (ret, message)
          }
          case Failure(exception) => (false, exception.getMessage)
        }

        if (!isValid) {
          configValid = false
          printf("Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
        }
      }

      if (!configValid) {
        System.exit(-1) // invalid configuration
      }
    }
  }

  private def write2Influx(
    sc: SparkContext,
    inputRecords: Long,
    inputBytes: Long,
    outputWritten: Long,
    outputBytes: Long): Unit = {

    val httpclient = HttpClients.createDefault

    val url = "http://192.168.22.63:8086/write?db=waterdrop_batch"
    val httpPost = new HttpPost(url){{
      addHeader(HTTP.CONTENT_TYPE, "BINARY")
    }}

    val tags = s"appId=${sc.applicationId},appName=${sc.appName}," +
      s"user=${sc.sparkUser},startTime=${sc.startTime},endTime=${System.currentTimeMillis}"
    val fields = s"inputRecords=${inputRecords},inputBytes=${inputBytes},outputWritten=${outputWritten},outputBytes=${outputBytes}"
    val text = s"waterdrop_report,${tags} ${fields}"
    println("[INFO] influx data:" + text)

    httpPost.setEntity(new StringEntity(text))
    val response = httpclient.execute(httpPost)

    println("[INFO] write 2 influxdb response:\n" + response)

  }

}
