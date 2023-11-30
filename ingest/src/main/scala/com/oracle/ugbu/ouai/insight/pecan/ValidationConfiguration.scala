package com.oracle.ugbu.ouai.insight.pecan

case class ValidationConfiguration(table: String,
                                   column: String,
                                   startingDate: String,
                                   endingDate: String,
                                   deltaFilePath: String,
                                   secretsPath: String,
                                   configPath: String)
