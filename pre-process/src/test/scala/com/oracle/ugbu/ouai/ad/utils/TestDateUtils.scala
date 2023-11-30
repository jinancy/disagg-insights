package com.oracle.ugbu.ouai.ad.utils

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class TestDateUtils extends FunSuite {

  test("isHoliday") {
    val timestamp = Timestamp.valueOf("2021-07-04 00:15:00")
    val isHoliday = DateUtils.isHoliday(timestamp, Seq("2021-07-04"))
    assert(isHoliday == 1)
  }

  test("get quarter") {
    val dt = "2021-07-05T23:05:12.000Z"
    // 2021-07-05 23:15
    val quarter = DateUtils.getQuarter(dt)
    println(quarter)
    assert(quarter != null)
  }

  test("get quarter for edge case") {
    val dt = "2021-07-05T23:48:00.000Z"
    //2021-07-06 00:00:00
    val quarter = DateUtils.getQuarter(dt)
    println(quarter)
    assert(quarter != null)
  }

  test("test quarter for the pecan st") {
    val dt = "2016-01-21T17:15:00Z"
    val quarter = DateUtils.getQuarter(dt)
    println(quarter)
  }

}
