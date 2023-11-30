package com.oracle.ugbu.ouai.insight.spark.streaming.lib.validation

import scala.collection.mutable

object Validator {
  def validateJdbcConfigs(extraOptions: mutable.HashMap[String, String]): Unit = {
    val possibleOptions: List[String] = List("user", "password", "url", "driver")
    val extraOptionsWithoutNulls = extraOptions.filter { case (k, v) => v != null }
    val missingOption = possibleOptions.find(s => !extraOptionsWithoutNulls.contains(s))
    if (missingOption.isDefined) {
      throw new RuntimeException(missingOption.get + " is missing")
    }
  }

  def validateStreamingConfigs(primaryKeys: Seq[String], offsetColumn: String, table: String): Unit = {
    if (offsetColumn == null) {
      throw new RuntimeException(offsetColumn + " is missing")
    } else if (table == null) {
      throw new RuntimeException(table + " is missing")
    } else if (primaryKeys.isEmpty) {
      throw new RuntimeException("primaryKeys is missing")
    }

  }
}
