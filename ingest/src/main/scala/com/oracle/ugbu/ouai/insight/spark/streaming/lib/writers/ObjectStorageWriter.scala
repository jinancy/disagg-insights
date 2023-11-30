package com.oracle.ugbu.ouai.insight.spark.streaming.lib.writers

import io.delta.tables.DeltaTable
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable


class ObjectStorageWriter(spark:SparkSession, outputPath: String, primaryKeys: Seq[String] ) {
  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  private var primaryKeyCondition : String = null
  private var allColumnNames: Array[String] = null
  private var columnToUpdate : Array[String] = null
  private var columnMapToUpdate : mutable.HashMap[String,String] = null
  private var columnMap : mutable.HashMap[String,String] = null

  def setPrimaryKeyCondition(): Unit ={
    for(primaryKey <- primaryKeys){
      if (primaryKeyCondition == null){
        primaryKeyCondition = "new."+primaryKey+" = old."+primaryKey
      }else{
        primaryKeyCondition += " and new."+primaryKey+" = old."+primaryKey
      }
    }

  }

  def outputToDelta(frame: sql.DataFrame): Unit ={
    allColumnNames = frame.columns
    columnToUpdate = allColumnNames.diff(primaryKeys)
    columnMapToUpdate = new scala.collection.mutable.HashMap[String, String]
    columnMap = new scala.collection.mutable.HashMap[String, String]

    for(column <- columnToUpdate){
      columnMapToUpdate += ArrowAssoc(column). -> ("new."+column)
    }

    for(column <- allColumnNames){
      columnMap += ArrowAssoc(column). -> ("new."+column)
    }

    if(DeltaTable.isDeltaTable(outputPath)) {
      DeltaTable.forPath(spark, outputPath).as("old")
        .merge(frame.as("new"), primaryKeyCondition)
        .whenMatched.updateExpr(columnMapToUpdate).whenNotMatched.insertExpr(columnMap)
        .execute()
    } else{
      logger.info("Creating delta table for the first time under path = " + outputPath)
      frame.write.mode("append").format("delta").save(outputPath)
    }
  }
}