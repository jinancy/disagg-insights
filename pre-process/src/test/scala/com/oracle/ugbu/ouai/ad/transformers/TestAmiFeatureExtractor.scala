package com.oracle.ugbu.ouai.ad.transformers

import com.oracle.ugbu.ouai.ad.constants.{ColumnName, Schemas}
import com.oracle.ugbu.ouai.ad.readers.DatasetReader
import com.oracle.ugbu.ouai.ad.utils.{DateUtils, TestSparkSessionUtils}
import org.apache.spark.sql.functions.{col, udf}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestAmiFeatureExtractor extends TestSparkSessionUtils {

  ignore("ami feature extraction") {
    val usaFederalHolidays = DatasetReader.readUsaFederalHolidays(spark,
      "./src/test/resources/usa_federal_holidays.csv")
    val metadata = DatasetReader.readMetadata(spark,
      "./src/test/resources/metadata.csv")
    val amiDF = DatasetReader.readPecanStAmi(spark,
      "./src/test/resources/ami_usage.csv", "batch", "csv")
    val features =  AmiFeatureExtractor.extractFeatures(amiDF, usaFederalHolidays,
      metadata, "innovation_lab")

    assert(features.count() == 96)
  }

  test("innovation lab feature extraction") {
    val sparkSession = spark
    import sparkSession.implicits._

    val quarterUdf = udf(DateUtils.getQuarter(_:String))
    val amiDF = spark.read.parquet("./src/test/resources/innovation_lab/ugbuouil/raw/electric_svc_type/*/*/*/*/*/*/")
      .filter($"meter_type" === "power_net")
      .withColumnRenamed("meter_id", ColumnName.DATA_ID)
      .withColumn(ColumnName.LOCAL_15MIN, quarterUdf($"time"))
      .withColumnRenamed("usage", ColumnName.GRID)
      .select(Schemas.AMI_SCHEMA.fieldNames.map(col):_*)


    amiDF.printSchema()
    amiDF.show(false)

    //meter_id -> dataid
    //time (string) -> local_15min (timestamp) -> aggregate to 15min interval
    // usage (double) -> GRID (change type from float to double)
  }

}
