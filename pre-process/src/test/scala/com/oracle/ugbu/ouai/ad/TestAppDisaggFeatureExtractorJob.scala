package com.oracle.ugbu.ouai.ad

import com.oracle.ugbu.ouai.ad.model.Configuration
import com.oracle.ugbu.ouai.ad.readers.DatasetReader
import com.oracle.ugbu.ouai.ad.utils.TestSparkSessionUtils

class TestAppDisaggFeatureExtractorJob extends TestSparkSessionUtils {

//  test("testing innovation lab") {
//    val sparkSession = spark
//    val inputPath = "./src/test/resources/innovation_lab/ugbuouil/raw/electric_svc_type/*/*/*/*/*/*/"
//    val mode = "batch"
//    val amiDF = DatasetReader.readInnovationLabAmi(spark, inputPath, mode)
//    println(amiDF.count())
//  }

  test("test pecan st functional test") {
    val sparkSession = spark
    val inputPath = "/Users/siva.tetala/ws/disagg/pecan_st_disagg/func_test/2/raw2/"
    val outputPath = "/Users/siva.tetala/ws/disagg/pecan_st_disagg/func_test/2/pre-process"
    val holidaysPath = "/Users/siva.tetala/ws/pecan_st_disagg/usa_federal_holidays.csv"
    val metadataPath = "/Users/siva.tetala/ws/disagg/metadata/metadata.csv"
    val mode = "stream"
    val source = "pecan"
    val config = Configuration(inputPath = inputPath,
      outputPath = outputPath, holidaysPath = holidaysPath, metadataPath = metadataPath,
      source = source, mode = mode, configPath = null)
    AppDisaggFeatureExtractorJob.run(config, sparkSession)
  }

}
