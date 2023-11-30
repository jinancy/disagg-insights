package com.oracle.ugbu.ouai.ad.model

import scala.collection.mutable

case class WindowState(var window: mutable.Queue[InputAmiRow]) {
  def process(inputAmiRows: Seq[InputAmiRow]) : Seq[OutputRow] = {
    var outputListBuffer = mutable.ListBuffer.empty[OutputRow]
    val sorted : Seq[InputAmiRow] = inputAmiRows
      .groupBy(_.local_15min)
      .map{ case (_, rows) =>
        val sum  = rows.map(_.total).foldLeft(0.00f)(_ + _)
        rows.head.copy(total = sum)
      }
      .toSeq
      .filter(p => Option(p.total).isDefined && p.total > 0.00f)
      .sortBy(_.local_15min.getTime)

    for (inputRow <- sorted) {
      val opt = updateStateAndOutputRow(inputRow)
      if (opt.dataid != null) {
        outputListBuffer += opt
      }
    }

    outputListBuffer
  }

//  def fillWindow(input: Seq[InputAmiRow]) : Seq[InputAmiRow] = {
//    val deltaSize = input.size
//    if (this.window.length + deltaSize < 96) {
//      val lastElement = input.last
//      val fillTo = (1 to (96 - (this.window.length + deltaSize))).map(i => lastElement)
//      input ++ fillTo
//    }
//    else {
//      input
//    }
//  }

  def updateStateAndOutputRow(input: InputAmiRow): OutputRow = {
    this.window.enqueue(input)
    if (this.window.length > 96) {
      this.window.dequeue()
    }
    if (this.window.length == 96) {
      val inputs: Seq[Seq[java.lang.Float]] = this.window.map(i => Seq(
        i.dayofmonth, i.dayofweek, i.dayofyear, i.dewPoint,
        i.is_holiday, i.month, i.temperature, i.total, i.weekofyear))
      val firstInputRow = this.window.front
      OutputRow(input.dataid, firstInputRow.local_15min, 15, firstInputRow.city, firstInputRow.state,  inputs)
    } else {
      OutputRow(null, null, null, null, null, null)
    }
  }
}
