package com.oracle.ugbu.ouai.insight.spark.streaming.lib.writers

import com.oracle.ugbu.ouai.insight.spark.streaming.lib.batch.JDBCBatchJob
import com.oracle.ugbu.ouai.insight.spark.streaming.lib.offset.CheckPointer
import com.oracle.ugbu.ouai.insight.spark.streaming.lib.sources.PostgresQueryBuilder
import com.oracle.ugbu.ouai.insight.spark.streaming.lib.validation.Validator.{validateJdbcConfigs, validateStreamingConfigs}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

class JdbcStreamingWriter (spark: SparkSession) {  //change class names

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
  private var primaryKeys : Seq[String] = null
  private var outputPath : String = null
  private var checkPointPath : String = null
  private var startingOffset :String =  "latest" // earliest, latest, specific value
  private var offsetColumn : String = null
  private var maxOffsetsPerTrigger : Int = 1000
  private var table : String = null
  private var triggerInterval : Int = 1000

  /**
   * possible options: driver, url, username, password (connectivity)
   */
  def option(key: String, value: String): JdbcStreamingWriter = {
    this.extraOptions += (key -> value)
    this
  }

  def primaryKeys(primaryKeys: Seq[String]): JdbcStreamingWriter = {
    this.primaryKeys = primaryKeys
    this
  }

  def outputPath(outputPath : String) : JdbcStreamingWriter = {
    this.outputPath = outputPath
    this
  }

  def checkPointPath(checkPointPath: String) : JdbcStreamingWriter = {
    this.checkPointPath = checkPointPath
    this
  }

  def startingOffset(startingOffset: String) : JdbcStreamingWriter = {
    this.startingOffset = startingOffset
    this
  }

  def offsetColumn(offsetColumn: String) : JdbcStreamingWriter = {
    this.offsetColumn = offsetColumn
    this
  }

  def maxOffsetsPerTrigger(maxOffsetsPerTrigger: Int) : JdbcStreamingWriter = {
    this.maxOffsetsPerTrigger = maxOffsetsPerTrigger
    this
  }

  def table(table: String) : JdbcStreamingWriter = {
    this.table = table
    this
  }

  def triggerInterval(triggerInterval: Int) : JdbcStreamingWriter = {
    this.triggerInterval = triggerInterval
    this
  }

  def start(): Unit = {
    // validate jdbc configs
    validateJdbcConfigs(extraOptions)

    // validate streaming configs
    validateStreamingConfigs(primaryKeys, offsetColumn, table )

    val checkpoint = new CheckPointer(spark,checkPointPath, startingOffset)
    startingOffset = checkpoint.getOffset()
    val makeQuery = new PostgresQueryBuilder(startingOffset, table, maxOffsetsPerTrigger, offsetColumn)
    var query = makeQuery.initialSQL()
    val batchJob = new JDBCBatchJob(spark, extraOptions)
    val toObjectStorage = new ObjectStorageWriter(spark, outputPath, primaryKeys)
    toObjectStorage.setPrimaryKeyCondition()

    runJDBC()

    @tailrec
    def runJDBC(): Unit ={
      val start = System.currentTimeMillis
      val dataFrame = batchJob.run(query)
      if(dataFrame.isEmpty) {
        val end = System.currentTimeMillis
        if ((end - start) < triggerInterval) {
          Thread.sleep(triggerInterval - (end - start))
        }
        runJDBC()
      }else{
        toObjectStorage.outputToDelta(dataFrame)
        query = makeQuery.updatedOffset(dataFrame)
        checkpoint.writeToCheckPoint(makeQuery.endingOffset, makeQuery.startingOffset, makeQuery.count)//where to get batch id
        val end = System.currentTimeMillis
        if ((end - start) < triggerInterval){
          Thread.sleep(triggerInterval - (end - start))
        }
        runJDBC()
      }
    }
  }
}

