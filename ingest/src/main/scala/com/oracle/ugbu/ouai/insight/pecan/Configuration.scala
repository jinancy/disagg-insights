package com.oracle.ugbu.ouai.insight.pecan

case class Configuration(primaryKeys : Seq[String],
                         offsetColumn : String,
                         startingOffset : String,
                         table : String,
                         maxOffsetsPerTrigger : String,
                         checkPointPath : String,
                         outputPath : String,
                         triggerInterval : String,
                         secretsPath : String,
                         configPath :String)
