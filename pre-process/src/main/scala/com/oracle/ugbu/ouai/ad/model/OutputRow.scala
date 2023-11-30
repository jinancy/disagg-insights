package com.oracle.ugbu.ouai.ad.model

case class OutputRow(dataid: String,
                     local_15min: java.sql.Timestamp, //make last time stamp
                     sliding_window: Integer,
                     city: String,
                     state: String,
                     feature_vector: Seq[Seq[java.lang.Float]])
