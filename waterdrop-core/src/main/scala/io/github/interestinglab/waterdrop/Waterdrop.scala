package io.github.interestinglab.waterdrop

import java.io.File

import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.utils.{AsciiArt, CompressionUtils}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Waterdrop extends Logging {

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

  private def showWaterdropAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Waterdrop")
  }

  private def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
  }

  private def showFatalError(throwable: Throwable): Unit = {
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

    var configValid = true
    val plugins = staticInputs ::: streamingInputs ::: filters ::: outputs
    for (p <- plugins) {
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

    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {

          logInfo("preparing cluster mode work dir files...")

          // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
          val workDir = new File(".")
          logWarning("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

          workDir.listFiles().foreach(f => logWarning("\t list file: " + f.getAbsolutePath))

          // decompress plugin dir
          val compressedFile = new File("plugins.tar.gz")

          Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
            case Success(tempFile) => {
              Try(CompressionUtils.unTar(tempFile, workDir)) match {
                case Success(_) => logInfo("succeeded to decompress plugins.tar.gz")
                case Failure(ex) => {
                  logError("failed to decompress plugins.tar.gz", ex)
                  sys.exit(-1)
                }
              }

            }
            case Failure(ex) => {
              logError("failed to decompress plugins.tar.gz", ex)
              sys.exit(-1)
            }
          }
        }
      }
    }

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

    val sparkConfig = configBuilder.getSparkConfigs
    val duration = sparkConfig.getLong("spark.streaming.batchDuration")
    val sparkConf = createSparkConf(configBuilder)
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

      //      val schema = StructType(Array(StructField("raw_message", StringType)))
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
        println(s"[INFO] input ${input.getClass.getSimpleName} done!!!")
        if (ds.columns.length > 0) {
          for (f <- filters) {
            ds = f.process(sparkSession, ds)
            println(s"[INFO] filter ${f.getClass.getSimpleName} done!!!")
          }
          outputs.foreach(p => {
            p.process(ds)
            println("[INFO] out put sum: " + ds.count)
          })
        }
        input.afterBatch
      }
    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }

  }

  private def basePrepare(
    sparkSession: SparkSession,
    staticInputs: List[BaseStaticInput],
    streamingInputs: List[BaseStreamingInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {
    for (i <- streamingInputs) {
      i.prepare(sparkSession)
    }

    basePrepare(sparkSession, staticInputs, filters, outputs)
  }

  private def basePrepare(
    sparkSession: SparkSession,
    staticInputs: List[BaseStaticInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    for (i <- staticInputs) {
      i.prepare(sparkSession)
    }

    for (o <- outputs) {
      o.prepare(sparkSession)
    }

    for (f <- filters) {
      f.prepare(sparkSession)
    }
  }

  private def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }
}
