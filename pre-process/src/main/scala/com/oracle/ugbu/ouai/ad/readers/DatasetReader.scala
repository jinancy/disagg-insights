package com.oracle.ugbu.ouai.ad.readers

import com.oracle.ugbu.ouai.ad.constants.{ColumnName, Schemas}
import com.oracle.ugbu.ouai.ad.model.Metadata
import com.oracle.ugbu.ouai.ad.utils.DateUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.control.Breaks
import scala.util.control.Breaks.break

object DatasetReader {

  def validatePath(spark: SparkSession, inputPath: String, format : String = "delta"): Unit = {
    Breaks.breakable {
      while (true) {
        try {
          spark.read.format(format).load(inputPath)
          println(f"$inputPath is available now to stream")
          break
        }
        catch {
          case _ : Exception =>
            println(f"${inputPath} is not present")
            Thread.sleep(1000)
        }
      }
    }
  }

  def readPecanStAmi(spark: SparkSession,
                     inputPath: String,
                     mode: String,
                     format : String = "delta"): Dataset[Row] = {
    val df = if (mode == "stream") {

      validatePath(spark, inputPath, format)

      spark.readStream
        .option("header", "true")
        .format(format)
        .option("ignoreChanges", "true")
        .option("maxFilesPerTrigger", "2")
        .load(inputPath)
    } else {
      spark.read
        .option("header", "true")
        .format(format)
        .load(inputPath)
    }

    handlePecanGrid(df)
      .withColumn(ColumnName.GRID, col(ColumnName.GRID).cast("float"))
      .filter(col(ColumnName.GRID).isNotNull && col(ColumnName.GRID) > 0.00)
      .withColumn(ColumnName.LOCAL_15MIN, col(ColumnName.LOCAL_15MIN).cast("timestamp"))
  }

  def handlePecanGrid(df: Dataset[Row]): Dataset[Row] = {
    val columns = df.columns
    if (columns.contains(ColumnName.GRID)) {
      df.select(Schemas.AMI_SCHEMA.fieldNames.map(col):_*)
    }
    else {
      if (columns.contains("grid_l1") && columns.contains("grid_l2")) {
        df
          .withColumn(ColumnName.GRID, (df.col("grid_l1") + df.col("grid_l2")) / 2)
          .select(Schemas.AMI_SCHEMA.fieldNames.map(col):_*)
      }
      else if (columns.contains("grid_l1")) {
        df.withColumnRenamed("grid_l1", ColumnName.GRID)
          .select(Schemas.AMI_SCHEMA.fieldNames.map(col):_*)
      }
      else if (columns.contains("grid_l2")) {
        df.withColumnRenamed("grid_l2", ColumnName.GRID)
          .select(Schemas.AMI_SCHEMA.fieldNames.map(col):_*)
      }
      else {
        throw new RuntimeException("GRID column is not present in Pecan St Source")
      }
    }
  }

  def readInnovationLabAmi(spark: SparkSession,
                           inputPath: String,
                           mode: String) : Dataset[Row] = {
    val df = if (mode == "stream") {
      spark.readStream
        .schema(Schemas.INNOVATION_LAB_SCHEMA)
        .parquet(inputPath)
    } else {
      spark.read
        .schema(Schemas.INNOVATION_LAB_SCHEMA)
        .parquet(inputPath)
    }
    df
      .filter(col(ColumnName.METER_ID) === "ZB:0013500501059b35") //filtering innovation lab meter
      .withColumnRenamed(ColumnName.METER_ID, ColumnName.DATA_ID)
      .withColumn(ColumnName.LOCAL_15MIN, DateUtils.quarterUdf(col(ColumnName.TIME)))
      .withColumn(ColumnName.GRID, col(ColumnName.USAGE).cast("float"))
      .filter(col(ColumnName.GRID).isNotNull && col(ColumnName.GRID) > 0.00)
      .select(Schemas.AMI_SCHEMA.fieldNames.map(col):_*)
  }

  def readUsaFederalHolidays(spark: SparkSession, holidaysPath: String) : Seq[String] = {
    spark.read
      .option("header", "true")
      .csv(holidaysPath)
      .collect()
      .map(_.getString(0))
  }

  def readMetadata(spark: SparkSession, metadataPath: String) : Map[String, Metadata] = {
    var metadataMap = scala.collection.mutable.HashMap[String, Metadata]()
    spark.read
      .option("header", "true")
      .csv(metadataPath)
      .distinct()
      .collect()
      .foreach{ case row =>
        val dataid = row.getString(row.fieldIndex("dataid"))
        val city = row.getString(row.fieldIndex("city"))
        val state = row.getString(row.fieldIndex("state"))
        if (!metadataMap.contains(dataid)) {
          metadataMap += (dataid -> Metadata(dataid = dataid, city=city, state=state))
        }
      }

    metadataMap.toMap
  }

}
