package com.oracle.ugbu.ouai.ad.transformers

import com.oracle.ugbu.ouai.ad.model.{InputAmiRow, OutputRow, WindowState}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.collection.mutable

object PreProcessor {

  def applySlidingWindow(amiFeaturesDF: Dataset[InputAmiRow], mode: String): Dataset[OutputRow] = {
    import amiFeaturesDF.sparkSession.implicits._

    if ("stream".equals(mode)) {
      amiFeaturesDF
        .groupByKey(_.dataid)
        .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(updateAcrossEvents)
    }
    else {
      amiFeaturesDF
        .groupByKey(_.dataid)
        .flatMapGroups{ case (_, groups) =>
          val emptyWindow =  WindowState(mutable.Queue.empty[InputAmiRow])
          val outputRows = emptyWindow.process(groups.toSeq)
          Seq(outputRows.last).toIterator
//          outputRows.toIterator
        }
    }
  }

  def updateAcrossEvents(dataid: String,
                         inputAmiRows: Iterator[InputAmiRow],
                         oldState: GroupState[WindowState]): Iterator[OutputRow] = {
    val state: WindowState = {
      if (oldState.exists) {
        oldState.get
      } else {
        WindowState(mutable.Queue.empty[InputAmiRow])
      }
    }
    val outputRows = state.process(inputAmiRows.toSeq)
    oldState.update(state)
    outputRows.toIterator
  }
}
