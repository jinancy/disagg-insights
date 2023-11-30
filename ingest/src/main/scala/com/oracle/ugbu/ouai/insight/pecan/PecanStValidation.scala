package com.oracle.ugbu.ouai.insight.pecan

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


class Validator(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
  private var column : String = null // I dont think we need this
  private var table : String = null
  private var startingDate : String = null
  private var endingDate : String = null
  private var deltaFilePath : String = null


  def option(key: String, value: String): Validator = {
    this.extraOptions += (key -> value)
    this
  }

  def column(column : String) : Validator = {
    this.column = column
    this
  }

  def table(table : String) : Validator = {
    this.table = table
    this
  }

  def startingDate(startingDate : String) : Validator = {
    this.startingDate = startingDate
    this
  }

  def endingDate(endingDate : String) : Validator = {
    this.endingDate = endingDate
    this
  }

  def deltaFilePath(deltaFilePath : String) : Validator = {
    this.deltaFilePath = deltaFilePath+"/deltaFiles"
    this
  }

  def dfPostgres(): DataFrame = {
    val query =  s"(select date($column) as day_id, count(*) as pecan_st_count, " +
      s"count(distinct dataid) as pecan_st_distinct_users_count from $table where " +
      s"$column between '$startingDate' and '$endingDate' group by date($column)) as t"

    val jdbcDF = spark.read.format("jdbc").options(
      Map("url" -> extraOptions("url"),
        "dbtable" -> query,
        "user" -> extraOptions("user"),
        "password" -> extraOptions("password"),
        "driver" -> extraOptions("driver"),
      )).load()
    return jdbcDF
  }

  def osDF(): DataFrame = {
    if(DeltaTable.isDeltaTable(deltaFilePath)) {
      val events = spark.read.format("delta").load(deltaFilePath)
      events.createOrReplaceTempView("deltaFiles")
      val deltaDF = spark.sql(s"(select date($column) as day_id, count(*) as delta_files_count, " +
        s"count(distinct dataid) as delta_files_distinct_users_count from deltaFiles where " +
        s"$column between '$startingDate' and '$endingDate' group by date($column))")
      deltaDF
    }
    else {
      spark.emptyDataFrame
    }
  }

  def run(): Unit ={
    val postgresDF = dfPostgres()
    val objectStorageDF = osDF()

    if(!postgresDF.isEmpty && !objectStorageDF.isEmpty) {
      val joinPostgresOS = postgresDF.join(objectStorageDF, postgresDF("day_id") === objectStorageDF("day_id")).drop(postgresDF("day_id"))
      joinPostgresOS.show()
    }

    val sumPostgres =  postgresDF.agg(sum("pecan_st_count")).first.get(0)
    logger.info("Total records in Postgres: " + sumPostgres)

    val sumOS =  objectStorageDF.agg(sum("delta_files_count")).first.get(0)
    logger.info("Total records in OS: " + sumOS)

    logger.info("Day ID's that weren't matched: ")
    postgresDF.join(objectStorageDF,postgresDF("day_id") === objectStorageDF("day_id"),"left_anti").show()
  }
}
