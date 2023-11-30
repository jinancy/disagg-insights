package com.oracle.ugbu.ouai.ad.utils

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, FunSuite, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class TestSparkSessionUtils extends FunSuite
  with Matchers
  with SharedSparkContext
  with ScalaCheckDrivenPropertyChecks {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().config(sc.getConf).getOrCreate()
  }

}
