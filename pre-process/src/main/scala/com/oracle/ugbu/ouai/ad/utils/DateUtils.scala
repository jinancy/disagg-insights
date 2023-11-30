package com.oracle.ugbu.ouai.ad.utils

import org.apache.spark.sql.functions.udf
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}

import java.sql.Timestamp

object DateUtils {

  def isHoliday(datetime : Timestamp, holidays: Seq[String] ) : Int = {
    val dt = new DateTime(datetime)
    if (holidays.contains(dt.toString("yyyy-MM-dd"))) 1 else 0
  }

  def getQuarter(dateTime : String) : Timestamp = {
    var dt : DateTime = null
    try{
      dt = ISODateTimeFormat.dateTime().withZoneUTC().parseDateTime(dateTime)
    }
    catch {
      case _ : Exception => {
        dt = DateTime.parse(dateTime, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"))
      }
    }

    val quarter = dt.getMinuteOfHour / 15
    val dtStr = if (quarter == 3) {
       dt.plusMinutes(15).toString("yyyy-MM-dd HH:") + "00:00"
    }
    else {
      dt.toString("yyyy-MM-dd HH:") + (quarter+1)*15 + ":00"
    }

    new Timestamp(DateTime.parse(dtStr, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
      .getMillis)
  }

  val quarterUdf = udf(DateUtils.getQuarter(_:String))
}
