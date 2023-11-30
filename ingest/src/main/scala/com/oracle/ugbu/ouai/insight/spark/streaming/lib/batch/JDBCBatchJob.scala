package com.oracle.ugbu.ouai.insight.spark.streaming.lib.batch

import com.oracle.ugbu.ouai.insight.spark.streaming.lib.models.JdbcConstants._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class JDBCBatchJob(spark: SparkSession, extraOptions: mutable.HashMap[String, String]) {
  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def run(query: String): DataFrame = {
    logger.info("Running JDBC Query: " + query)
    val jdbcDF = spark.read.format("jdbc").options(
      Map(url -> extraOptions("url"),
        dbtable -> query,
        user -> extraOptions("user"),
        password -> extraOptions("password"),
        driver -> extraOptions("driver"),
      ))
      .load()
    jdbcDF
  }

}
