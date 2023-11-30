package com.oracle.ugbu.ouai.insight.pecan

import com.oracle.ugbu.ouai.insight.utils.SparkSessionUtils
import org.apache.commons.cli._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object PecanStrValidator {
  private val logger: Logger = LoggerFactory.getLogger(PecanStrValidator.getClass)

  def parseArguments(args: Array[String]): ValidationConfiguration = {

    // create the command line parser
    val parser: CommandLineParser = new BasicParser()
    var errorArgsList = ArrayBuffer[String]()

    // create the Options
    val options: Options = new Options()

    // add arguments options
    options.addOption("t", "table", true, "Table")
    options.addOption("column", "column", true, "Column")
    options.addOption("startingDate", "startingDate", true, "Starting Date")
    options.addOption("endingDate", "endingDate", true, "Ending Date")
    options.addOption("d", "deltaFilePath", true, "Delta file path")
    options.addOption("s", "secretsPath", true, "Secrets Path delta file path")
    options.addOption("c", "configPath", true, "Config files path")
    options.addOption("h", "help", false, "Shows the help")


    var table: String = ""
    var column: String = ""
    var startingDate: String = ""
    var endingDate: String = ""
    var deltaFilePath: String = ""
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

      table = commandline.getOptionValue("t")
      column = commandline.getOptionValue("column")
      startingDate= commandline.getOptionValue("startingDate")
      endingDate = commandline.getOptionValue("endingDate")
      deltaFilePath = commandline.getOptionValue("d")
      secretsPath = commandline.getOptionValue("s")
      configPath = commandline.getOptionValue("c")

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
    if (table.isEmpty) {
      errorArgsList += "table"
    }
    if (column.isEmpty) {
      errorArgsList += "column"
    }
    if (startingDate.isEmpty) {
      errorArgsList += "startingDate"
    }
    if (endingDate.isEmpty) {
      errorArgsList += "endingDate"
    }
    if (deltaFilePath.isEmpty) {
      errorArgsList += "deltaFilePath"
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
    ValidationConfiguration(table, column, startingDate, endingDate, deltaFilePath,
      secretsPath, configPath)
  }

  def run(config: ValidationConfiguration, spark:SparkSession){
    // get options from config.secretPath
    val jdbcConfigsDF = spark.read.format("delta").load(config.secretsPath)
    val user = jdbcConfigsDF.select("user").head().getString(0)
    val password = jdbcConfigsDF.select("password").head().getString(0)
    val url = jdbcConfigsDF.select("url").head().getString(0)

    new Validator(spark)
      .option("user",user)
      .option("password",password)
      .option("url",url)
      .option("driver","org.postgresql.Driver")
      .column(config.column)
      .table(config.table)
      .startingDate(config.startingDate)
      .endingDate(config.endingDate)
      .deltaFilePath(config.deltaFilePath)
      .run()
  }

  def main(args: Array[String]): Unit = {
    val config = parseArguments(args)
    val spark = SparkSessionUtils.createSparkSession(config.configPath)
    run(config, spark)
  }
}
