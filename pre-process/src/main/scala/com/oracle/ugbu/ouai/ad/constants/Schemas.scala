package com.oracle.ugbu.ouai.ad.constants

import org.apache.spark.sql.types.StructType

object Schemas {

  val AMI_SCHEMA = new StructType()
    .add(ColumnName.DATA_ID, "string")
    .add(ColumnName.LOCAL_15MIN, "timestamp")
    .add(ColumnName.GRID, "float")

  val INNOVATION_LAB_SCHEMA = new StructType()
    .add(ColumnName.METER_ID, "string")
    .add(ColumnName.METER_TYPE, "string")
    .add(ColumnName.TIME, "string")
    .add(ColumnName.USAGE, "double")

}
