package com.oracle.ugbu.ouai.insight.spark.streaming.lib.offset

import com.oracle.ugbu.ouai.insight.spark.streaming.lib.models.{CheckPoint, CheckPointConstants}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.slf4j.LoggerFactory

class CheckPointer(spark:SparkSession, checkPointPath: String, initialOffset: String){
  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  var batchId :Long = 0

  def getOffset(): String = {
    try{
      val checkPointDF = spark.read.format("delta").load(checkPointPath)// create table by path
      if (checkPointDF.isEmpty) {
        logger.info("Check point is empty. Starting from offset "+ initialOffset)
        initialOffset
      } else {
        val maxBatch = checkPointDF.agg(max(CheckPointConstants.batchId)).head().getLong(0)
        logger.info("resuming from checkpoint")
        logger.info(s"last batch Id from Checkpoint: $maxBatch")
        batchId = maxBatch
        val newOffset = checkPointDF.filter(col("batchId")=== maxBatch).select("endingOffset").head().getString(0)
        logger.info(s"last offset from Checkpoint: $newOffset")
        newOffset
      }
    } catch {
      case _:AnalysisException => {
        logger.info("Checkpoint is empty. Starting from offset : "+initialOffset)
        initialOffset
      }
    }
  }

  def writeToCheckPoint(updatedOffset: String, startingOffset: String, count: Long): Unit = {
    import spark.implicits._
    batchId += 1
    val checkPoint = CheckPoint(batchId, startingOffset, updatedOffset, count)
    logger.info("Writing Checkpoint: " + checkPoint.toString)
    val checkPointDF = spark.createDataset(Seq(checkPoint)).toDF()
    checkPointDF.write.format("delta").mode("append").save(checkPointPath)
  }
}
