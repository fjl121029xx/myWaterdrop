package io.github.interestinglab.waterdrop

import java.util
import java.util.concurrent.{ExecutorService, Future, SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.metrics.OutputMetrics
import io.github.interestinglab.waterdrop.output.{Kafka, Mysql}
import io.github.interestinglab.waterdrop.utils.AsciiArt
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source2.HllStatBatchErrorSource
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object Waterdrop extends Logging {

  var threadPool:ExecutorService = _
  var viewTableMap: Map[String, String] = Map[String, String]()

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
            entrypoint(configFilePath)

            //            Try(entrypoint(configFilePath)) match {
            //              case Success(_) => {}
            //              case Failure(exception) => {
            //                exception.isInstanceOf[ConfigRuntimeException] match {
            //                  case true => {
            //                    showConfigError(exception)
            //                  }
            //                  case false => {
            //                    showFatalError(exception)
            //                  }
            //                }
            //              }
            //            }
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
    //=== 本地测试用
    sparkConf.setIfMissing("spark.master", "local") //↓
    sparkConf.setIfMissing("spark.app.name", "Waterdrop-local") //↑

    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    streamingInputs.size match {
      case 0 => {
        batchProcessing(sparkSession, configBuilder, staticInputs, filters, outputs)
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

    threadPool = new ThreadPoolExecutor(30,
      Integer.MAX_VALUE,
      60L, TimeUnit.SECONDS,
      new SynchronousQueue[Runnable]())

    val sparkConfig = configBuilder.getSparkConfigs
    val duration = sparkConfig.getLong("spark.streaming.batchDuration")
//    val sparkConf = createSparkConf(configBuilder)
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))

    //=== 20101029
    val sc = ssc.sparkContext
    logInfo("[Application ID] " + sc.applicationId)
    logInfo("[Application NAME] " + sc.appName)

    //=== 注册metrics
    val accumulators: util.HashMap[String, LongAccumulator] = OutputMetrics.registeMetrics(ssc, outputs)
    logInfo("had registered accumulators" + accumulators)
    //    basePrepare(sparkSession, staticInputs, streamingInputs, filters, outputs)
    //=== output 配置 metrics
    OutputMetrics.basePrepareWithMetrics(sparkSession, accumulators, staticInputs, streamingInputs, filters, outputs)

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


      for (f <- filters) {
        ds = f.process(sparkSession, ds)
      }

      streamingInputs.foreach(p => {
        p.beforeOutput
      })

      val ls: ListBuffer[Future[_]] = ListBuffer()
      try outputs.foreach(p => {
        p.setDf(ds)
        ls.add(threadPool.submit(p))
      }) catch {
        case ex: Exception => throw ex
      }
      ls.foreach(_.get)
      ls.clear()

      ds.unpersist()

      streamingInputs.foreach(p => {
        p.afterOutput
      })
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

    // let static input register as table for later use if needed
    registerInputTempView(staticInputs, sparkSession)

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
            var fds = ds
            fds = p.filterProcess(fds)
            p.process(fds)
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

  private[waterdrop] def registerInputTempView(staticInputs: List[BaseStaticInput], sparkSession: SparkSession): Unit = {
    for (input <- staticInputs) {
      val ds = input.getDataset(sparkSession)
      registerInputTempView(input, ds)
    }
  }

  private[waterdrop] def registerInputTempView(input: BaseStaticInput, ds: Dataset[Row]): Unit = {
    val config = input.getConfig()
    config.hasPath("table_name") || config.hasPath("result_table_name") match {
      case true => {
        val tableName = config.hasPath("table_name") match {
          case true => {
            @deprecated
            val oldTableName = config.getString("table_name")
            oldTableName
          }
          case false => config.getString("result_table_name")
        }
        registerTempView(tableName, ds)
      }

      case false => {
        //do nothing
      }
    }
  }

  private[waterdrop] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    viewTableMap.contains(tableName) match {
      case true =>
        throw new ConfigRuntimeException(
          "Detected duplicated Dataset["
            + tableName + "], it seems that you configured result_table_name = \"" + tableName + "\" in multiple static inputs")
      case _ => {
        ds.createOrReplaceTempView(tableName)
        viewTableMap += (tableName -> "")
      }
    }
  }

}
