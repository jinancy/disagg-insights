package com.oracle.ugbu.ouai.insight.utils

import com.oracle.ugbu.ouai.insight.constants.OCIConstants
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.TimeZone

object SparkSessionUtils {

  val logger: Logger = LoggerFactory.getLogger(SparkSessionUtils.getClass.getSimpleName)

  def createSparkSession(configFilePath: String): SparkSession = {
    logger.info("---------->>> Create OCI SparkSession <<<----------")

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val osConfig = ConfigFactory.parseFile(new File(configFilePath))

    logger.info("---->>> OS config object is -->> {}", osConfig.entrySet())
    val conf = new SparkConf()
      .setAppName("SpeedRacer")
      .set(
        "fs.oci.client.hostname",
        osConfig.getString(OCIConstants.OS_HOSTNAME)
      )
      .set(
        "fs.oci.client.auth.tenantId",
        osConfig.getString(OCIConstants.TENANCY_ID)
      )
      .set(
        "fs.oci.client.auth.userId",
        osConfig.getString(OCIConstants.USER_ID)
      )
      .set(
        "fs.oci.client.auth.fingerprint",
        osConfig.getString(OCIConstants.FINGERPRINT)
      )
      .set(
        "fs.oci.client.auth.pemfilepath",
        osConfig.getString(OCIConstants.PEM_FILE_PATH)
      )
      .set("fs.bmc.impl", "com.oracle.bmc.hdfs.BmcFilesystem")
      .set(
        "spark.delta.logStore.class",
        "org.apache.spark.sql.delta.storage.OciSingleDriverLogStore"
      )
      .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .set("spark.sql.broadcastTimeout", "600s")

    val sparkSsn = SparkSession.builder().config(conf)
      .master("local[*]")
      .getOrCreate()

    sparkSsn.sparkContext.hadoopConfiguration.set(
      "fs.oci.client.hostname",
      osConfig.getString(OCIConstants.OS_HOSTNAME)
    )
    sparkSsn.sparkContext.hadoopConfiguration.set(
      "fs.oci.client.auth.tenantId",
      osConfig.getString(OCIConstants.TENANCY_ID)
    )
    sparkSsn.sparkContext.hadoopConfiguration.set(
      "fs.oci.client.auth.userId",
      osConfig.getString(OCIConstants.USER_ID)
    )
    sparkSsn.sparkContext.hadoopConfiguration.set(
      "fs.oci.client.auth.fingerprint",
      osConfig.getString(OCIConstants.FINGERPRINT)
    )
    sparkSsn.sparkContext.hadoopConfiguration.set(
      "fs.oci.client.auth.pemfilepath",
      osConfig.getString(OCIConstants.PEM_FILE_PATH)
    )
    sparkSsn.sparkContext.hadoopConfiguration
      .set("namespace", osConfig.getString(OCIConstants.NAMESPACE))

    logger.info("---->>> Spark config object is -->> {}", sparkSsn.conf.getAll)

    logger.info(
      "------------------>>> SPARK SESSION CREATED <<<------------------"
    )
//    sparkSsn.sparkContext.setLogLevel("INFO")
    sparkSsn
  }

}
