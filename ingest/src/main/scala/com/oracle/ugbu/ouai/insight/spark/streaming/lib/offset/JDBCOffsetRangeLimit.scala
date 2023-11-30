package com.oracle.ugbu.ouai.insight.spark.streaming.lib.offset

sealed trait JDBCOffsetRangeLimit //extends Enumeration
case object EarliestOffsetRangeLimit extends JDBCOffsetRangeLimit {
  override val toString: String = "earliest"
}
case object LatestOffsetRangeLimit extends JDBCOffsetRangeLimit {
  override val toString: String = "latest"
}
case class SpecificOffsetRangeLimit[Unit](offset: Unit) extends JDBCOffsetRangeLimit {
  override def toString: String = offset.toString
}
