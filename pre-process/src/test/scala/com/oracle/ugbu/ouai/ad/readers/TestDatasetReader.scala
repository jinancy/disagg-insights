package com.oracle.ugbu.ouai.ad.readers

import com.oracle.ugbu.ouai.ad.utils.TestSparkSessionUtils

class TestDatasetReader extends TestSparkSessionUtils {

  test("read usa federal holidays") {
    val usaFederalHolidays = DatasetReader.readUsaFederalHolidays(spark,
      "./src/test/resources/usa_federal_holidays.csv")
    assert(usaFederalHolidays.length == 2294)
  }

}
