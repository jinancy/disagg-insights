package com.oracle.ugbu.ouai.insight.pecan

import com.oracle.ugbu.ouai.insight.spark.streaming.lib.writers.JdbcStreamingWriter
import com.oracle.ugbu.ouai.insight.utils.{CryptoUtils, SparkSessionUtils}
import org.apache.commons.cli._
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object PecanStDataIngester{

  val logger: Logger = LoggerFactory.getLogger(PecanStDataIngester.getClass)

  def parseArguments(args: Array[String]): Configuration = {

    // create the command line parser
    val parser: CommandLineParser = new BasicParser()
    var errorArgsList = ArrayBuffer[String]()

    // create the Options
    val options: Options = new Options()

    // add arguments options
    options.addOption("primaryKeys", "primaryKeys", true, "Primary keys list")
    options.addOption("offsetColumn", "offsetColumn", true, "Offset column")
    options.addOption("startingOffset", "startingOffset", true,
      "starting Offset can be either 'earliest', 'latest', or 'custom input'. Default: 'latest'")
    options.addOption("table", "table", true, "Table name")
    options.addOption("maxOffsetsPerTrigger", "maxOffsetsPerTrigger", true,
      "Max offsets per trigger. Defualt: '1000'")
    options.addOption("checkPointPath", "checkPointPath", true, "Check point files path")
    options.addOption("outputPath", "outputPath", true, "Output files path")
    options.addOption("triggerInterval", "triggerInterval", true,
      "Trigger interval in milliseconds. Default: '1000'")
    options.addOption("secretsPath", "secretsPath", true, "Secrets Path delta file path")
    options.addOption("configPath", "configPath", true, "Config files path")
    options.addOption("h", "help", false, "Shows the help")


    var primaryKeys: Seq[String] = null
    var offsetColumn: String = ""
    var startingOffset: String = ""
    var table: String = ""
    var maxOffsetsPerTrigger: String = ""
    var checkPointPath: String = ""
    var outputPath: String = ""
    var triggerInterval: String = ""
    var secretsPath: String = ""
    var configPath: String = ""

    try {
      // Parsing the command line arguments
      val commandline: CommandLine = parser.parse(options, args)

      if (commandline.hasOption("h")) {
        // print the help options
        val formatter: HelpFormatter = new HelpFormatter()
        formatter.printHelp("Command Line Parameters available>", options)
      }

      primaryKeys = commandline.getOptionValue("primaryKeys").split(",")
      offsetColumn = commandline.getOptionValue("offsetColumn")
      startingOffset = commandline.getOptionValue("startingOffset", "latest")
      table = commandline.getOptionValue("table")
      maxOffsetsPerTrigger = commandline.getOptionValue("maxOffsetsPerTrigger", "1000")
      checkPointPath = commandline.getOptionValue("checkPointPath")
      outputPath = commandline.getOptionValue("outputPath")
      triggerInterval = commandline.getOptionValue("triggerInterval", "1000")
      secretsPath = commandline.getOptionValue("secretsPath")
      configPath = commandline.getOptionValue("configPath")

    } catch {
      // ParseException Caught - When Invalid Options are provided.
      case e: org.apache.commons.cli.ParseException => {
        logger.error("Exception : Improper Arguments Provided")
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
    if (primaryKeys.isEmpty) {
      errorArgsList += "primaryKeys"
    }
    if (offsetColumn.isEmpty) {
      errorArgsList += "offsetColumn"
    }
    if (table.isEmpty) {
      errorArgsList += "table"
    }
    if (maxOffsetsPerTrigger.isEmpty) {
      errorArgsList += "maxOffsetsPerTrigger"
    }
    if (checkPointPath.isEmpty) {
      errorArgsList += "checkPointPath"
    }
    if (outputPath.isEmpty) {
      errorArgsList += "outputPath"
    }
    if (triggerInterval.isEmpty) {
      errorArgsList += "triggerInterval"
    }
    if (secretsPath.isEmpty) {
      errorArgsList += "secretsPath"
    }
    if (configPath.isEmpty) {
      errorArgsList += "configPath"
    }

    if (errorArgsList.nonEmpty) {
      logger.error("All the Mandatory Arguments Not Provided ... Termintating !!")
      logger.error("Missing Mandatory Arguments : ")
      for (missing_arguments <- errorArgsList) {
        logger.error(" " + missing_arguments)
      }
      val missingArguments = errorArgsList.mkString(", ")
      throw new org.apache.commons.cli.ParseException(
        "Exception: Missing Mandatory Arguments !!! : " + missingArguments
      )
    }
    Configuration(primaryKeys, offsetColumn, startingOffset, table, maxOffsetsPerTrigger,
          checkPointPath, outputPath, triggerInterval, secretsPath, configPath)
  }

  def run(config: Configuration, spark: SparkSession): Unit= {
    logger.info("Run Configuration: " + config)
    val cryptoUtils = CryptoUtils("ugbu_speedracer_pecan_st")
    logger.info("Started reading secrets ")
    val jdbcConfigs = spark.read.format("delta").load(config.secretsPath).head()
    val user = cryptoUtils.decrypt(jdbcConfigs.getString(jdbcConfigs.fieldIndex("username")))
    val password = cryptoUtils.decrypt(jdbcConfigs.getString(jdbcConfigs.fieldIndex("password")))
    val url = jdbcConfigs.getString(jdbcConfigs.fieldIndex("jdbc_url"))
    logger.info("Finished reading and decrypting secrets")

    new JdbcStreamingWriter(spark)
      .option("user", user)
      .option("password", password)
      .option("url", url)
      .option("driver", "org.postgresql.Driver")
      .primaryKeys(config.primaryKeys)
      .offsetColumn(config.offsetColumn)
      .startingOffset(config.startingOffset)
      .table(config.table)
      .maxOffsetsPerTrigger(config.maxOffsetsPerTrigger.toInt)
      .checkPointPath(config.checkPointPath)
      .outputPath(config.outputPath)
      .triggerInterval(config.triggerInterval.toInt)
      .start()
  }

  def main(args: Array[String]): Unit = {
    try {
      org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
      org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF)
      org.apache.log4j.Logger.getLogger("com.oracle.bmc").setLevel(Level.OFF)
      org.apache.log4j.Logger.getRootLogger.setLevel(Level.WARN)
      val config = parseArguments(args)
      val spark = SparkSessionUtils.createSparkSession(config.configPath)
      run(config, spark)
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
