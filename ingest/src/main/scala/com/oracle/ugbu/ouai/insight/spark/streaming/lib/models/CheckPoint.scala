package com.oracle.ugbu.ouai.insight.spark.streaming.lib.models

case class CheckPoint(batchId: Long, startingOffset: String, endingOffset: String, count: Long)
