package com.oracle.ugbu.ouai.ad.transformers

import com.oracle.ugbu.ouai.ad.constants.ColumnName
import com.oracle.ugbu.ouai.ad.model.{InputAmiRow, Metadata}
import com.oracle.ugbu.ouai.ad.utils.DateUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

import java.sql.Timestamp
import scala.util.Random

object AmiFeatureExtractor {

  def extractFeatures(amiDF: Dataset[Row],
                      holidays: Seq[String],
                      metadataMap: Map[String, Metadata],
                      source : String): Dataset[InputAmiRow] = {
    val spark = amiDF.sparkSession
    import spark.implicits._

    val broadcastHolidays = spark.sparkContext.broadcast(holidays)

    val isHolidayUDF = udf(DateUtils.isHoliday(_: Timestamp, broadcastHolidays.value))
    val cityUDF = udf(getCity(_:String, metadataMap))
    val stateUDF = udf(getState(_:String, metadataMap))

    // in Fahrenheit
    val (minTemp, maxTemp) = {
      if ("innovation_lab".equalsIgnoreCase(source)) {
        (43, 73)
      }
      else {
        (32, 83)
      }
    }

    val (minDew, maxDew) = {
      if ("innovation_lab".equalsIgnoreCase(source)) {
        (38, 65)
      }
      else {
        (28, 43)
      }
    }

    //TODO: add random floats for temperature and dewPoint temporarily
    val addTemperatureUDF = udf({ () => minTemp + Random.nextInt((maxTemp - minTemp) + 1)})
    val addDewPointUDF = udf({() => minDew + Random.nextInt((maxDew - minDew) + 1)})

    amiDF
      .withColumnRenamed(ColumnName.GRID, ColumnName.TOTAL)
      .withColumn(ColumnName.IS_HOLIDAY, isHolidayUDF(col(ColumnName.LOCAL_15MIN)))
      .withColumn(ColumnName.TEMPERATURE, addTemperatureUDF())
      .withColumn(ColumnName.DEW_POINT, addDewPointUDF())
      .withColumn(ColumnName.DAY_OF_MONTH, dayofmonth(col(ColumnName.LOCAL_15MIN)))
      .withColumn(ColumnName.DAY_OF_WEEK, dayofweek(col(ColumnName.LOCAL_15MIN)))
      .withColumn(ColumnName.DAY_OF_YEAR, dayofyear(col(ColumnName.LOCAL_15MIN)))
      .withColumn(ColumnName.MONTH, month(col(ColumnName.LOCAL_15MIN)))
      .withColumn(ColumnName.WEEK_OF_YEAR, weekofyear(col(ColumnName.LOCAL_15MIN)))
      .select(
        ColumnName.DATA_ID, ColumnName.LOCAL_15MIN, ColumnName.DAY_OF_MONTH,
        ColumnName.DAY_OF_WEEK, ColumnName.DAY_OF_YEAR, ColumnName.DEW_POINT,
        ColumnName.IS_HOLIDAY, ColumnName.MONTH, ColumnName.TEMPERATURE,
        ColumnName.TOTAL, ColumnName.WEEK_OF_YEAR
      )
      .withColumn(ColumnName.CITY, cityUDF(col(ColumnName.DATA_ID)))
      .withColumn(ColumnName.STATE, stateUDF(col(ColumnName.DATA_ID)))
      .as[InputAmiRow]
  }

  def getCity(dataid: String, metadataMap: Map[String, Metadata]) : String = {
    if (metadataMap.contains(dataid)) {
      metadataMap(dataid).city
    }
    else {
      "UA"
    }
  }

  def getState(dataid: String, metadataMap: Map[String, Metadata]) : String = {
    if (metadataMap.contains(dataid)) {
      metadataMap(dataid).state
    }
    else {
      "UA"
    }
  }

}
