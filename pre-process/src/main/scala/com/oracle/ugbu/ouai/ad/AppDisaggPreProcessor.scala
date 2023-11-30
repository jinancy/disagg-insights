package com.oracle.ugbu.ouai.ad


import com.oracle.ugbu.ouai.ad.model.Configuration
import com.oracle.ugbu.ouai.ad.utils.SparkSessionUtils
import org.apache.commons.cli._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object AppDisaggPreProcessor {

  val logger: Logger = LoggerFactory.getLogger(AppDisaggPreProcessor.getClass)

  def parseArguments(args: Array[String]): Configuration =  {

    // create the command line parser
    val parser: CommandLineParser = new BasicParser()
    var errorArgsList = ArrayBuffer[String]()

    // create the Options
    val options: Options = new Options()

    // add arguments options
    options.addOption("i", "inputPath", true, "Input files path")
    options.addOption("o", "outputPath", true, "Insights Output files path")
    options.addOption("c", "configPath", true, "Config files path")
    options.addOption("f", "holidayPath", true, "Holidays Path")
    options.addOption("s", "source", true, "Source could be pecan or innovation_lab")
    options.addOption("e", "metadata", true, "Metadata containing city/state mapping")
    options.addOption("m", "mode", true,
      "Mode can be either 'batch' or 'stream'. Default: 'stream'")
    options.addOption("h", "help", false, "Shows the help")

    var configPath : String = ""
    var outputPath : String = ""
    var inputPath : String = ""
    var holidaysPath : String = ""
    var metadataPath : String = ""
    var mode : String = ""
    var source : String = ""

    try {
      // Parsing the command line arguments
      val commandline: CommandLine = parser.parse(options, args)

      if (commandline.hasOption("h")) {
        // print the help options
        val formatter: HelpFormatter = new HelpFormatter()
        formatter.printHelp("Command Line Parameters available>", options)
      }

      configPath = commandline.getOptionValue("c")
      outputPath = commandline.getOptionValue("o")
      inputPath = commandline.getOptionValue("i")
      holidaysPath = commandline.getOptionValue("f")
      mode = commandline.getOptionValue("m", "stream")
      source = commandline.getOptionValue("s")
      metadataPath = commandline.getOptionValue("e")

    } catch {
      // ParseException Caught - When Invalid CLI Options are provided.
      case e: org.apache.commons.cli.ParseException => {
        logger.error("Exception : Improper CLI Arguments Provided")
        // print all the available options
        val formatter: HelpFormatter = new HelpFormatter()
        formatter.printHelp(
          "Possible Command Line Parameters allowed>",
          options
        )
        throw e
      }
    }

    // Validating if mandatory Arguments Are Provided.. ParseException if not available.
    if (configPath.isEmpty) {
      errorArgsList += "configPath"
    }
    if (outputPath.isEmpty) {
      errorArgsList += "outputPath"
    }
    if (inputPath.isEmpty) {
      errorArgsList += "inputPath"
    }
    if (holidaysPath.isEmpty) {
      errorArgsList += "holidaysPath"
    }

    if (source.isEmpty) {
      errorArgsList += "source"
    }

    if (metadataPath.isEmpty) {
      errorArgsList += "metadataPath"
    }

    if (!Seq("batch", "stream").contains(mode.toLowerCase())) {
      errorArgsList += "mode"
    }

    if (errorArgsList.nonEmpty) {
      logger.error("All the Mandatory Arguments Not Provided ... Termintating !!")
      logger.error("Missing Mandatory Arguments : ")
      for (missing_arguments <- errorArgsList) {
        logger.error(" " + missing_arguments)
      }
      val missingArguments = errorArgsList.mkString(",")
      throw new org.apache.commons.cli.ParseException(
        "Exception: Missing Mandatory Arguments !!! : " + missingArguments
      )
    }

    Configuration(inputPath, outputPath, configPath, holidaysPath, metadataPath, mode, source)
  }


  def main(args: Array[String]): Unit = {
    try {
      val config = parseArguments(args)
      val spark = SparkSessionUtils.createSparkSession(config.configPath)
      AppDisaggFeatureExtractorJob.run(config, spark)
    }
    catch {
      case e : Exception => {
        logger.error("Exception : " + e.getMessage)
        logger.error("StackTrace: " + e.getStackTrace.mkString("Array(", ", ", ")"))
        throw e
      }
    }

  }

}
