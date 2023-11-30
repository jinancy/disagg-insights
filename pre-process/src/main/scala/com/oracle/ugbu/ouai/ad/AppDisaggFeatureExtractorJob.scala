package com.oracle.ugbu.ouai.ad

import com.oracle.ugbu.ouai.ad.model.Configuration
import com.oracle.ugbu.ouai.ad.readers.DatasetReader
import com.oracle.ugbu.ouai.ad.transformers.{AmiFeatureExtractor, PreProcessor}
import com.oracle.ugbu.ouai.ad.writers.ObjectStorageWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object AppDisaggFeatureExtractorJob {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def run(config: Configuration, spark: SparkSession): Unit = {
    logger.info("Started reading USA Federal Holidays")
    val holidays = DatasetReader.readUsaFederalHolidays(spark, config.holidaysPath)
    logger.info("Finished reading USA Federal Holidays")
    logger.info("Reading AMI data for source : " + config.source)

    logger.info("Started reading metadata")
    val metadata = DatasetReader.readMetadata(spark, config.metadataPath)
    logger.info("Finished reading metadata")

    logger.info("Reading AMI data for source : " + config.source)
    val amiDF: Dataset[Row] = {
      if ("pecan".equalsIgnoreCase(config.source)) {
        DatasetReader.readPecanStAmi(spark, config.inputPath, config.mode)
      }
      else {
        // if ("innovation_lab".equalsIgnoreCase(config.source))
        DatasetReader.readInnovationLabAmi(spark, config.inputPath, config.mode)
      }
    }

    logger.info("Building features for the model")
    val amiFeaturesDF = AmiFeatureExtractor.extractFeatures(amiDF, holidays, metadata, config.source)

    logger.info("Applying sliding window")
    val slidingWindowDF = PreProcessor.applySlidingWindow(amiFeaturesDF, config.mode)

    logger.info("Writing output to OS")
    ObjectStorageWriter.write(slidingWindowDF, config.outputPath, config.mode)
  }

}
