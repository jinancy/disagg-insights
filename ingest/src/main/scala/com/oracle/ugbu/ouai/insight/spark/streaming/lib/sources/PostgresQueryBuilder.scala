package com.oracle.ugbu.ouai.insight.spark.streaming.lib.sources

import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql
import org.apache.spark.sql.functions.max
import org.slf4j.LoggerFactory


//sealed trait JDBCOffsetQueryBuilder

class PostgresQueryBuilder[Unit](offset: String, table: String, maxOffsetsPerTrigger: Int, offsetColumn: String){
  private val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  BasicConfigurator.configure()
  var endingOffset : String = offset
  var startingOffset : String = offset
  var count : Long = 0

  def initialSQL(): String = {
    val sqlCommand = {
      if (offset == "earliest") {
        s"(select * from $table order by $offsetColumn limit $maxOffsetsPerTrigger) as t "
      } else if (offset == "latest") {
        s"(select * from $table order by $offsetColumn desc limit $maxOffsetsPerTrigger) as t "
      }
      else {
        s"(select * from $table where $offsetColumn >= TIMESTAMP '$offset' order by $offsetColumn limit $maxOffsetsPerTrigger) as t"
      }
    }

    logger.info(s"Initial Query: $sqlCommand")
    sqlCommand
  }

  def updatedOffset(frame: sql.DataFrame): String = {
    startingOffset = endingOffset
    endingOffset = frame.agg(max(offsetColumn)).head().getTimestamp(0).toString
    count = frame.count()
    val sqlCommand = s"(select * from $table where $offsetColumn >= TIMESTAMP '$endingOffset' order by $offsetColumn limit $maxOffsetsPerTrigger) as t"
    logger.info(s"SQL Query: $sqlCommand")
    sqlCommand
  }
}

