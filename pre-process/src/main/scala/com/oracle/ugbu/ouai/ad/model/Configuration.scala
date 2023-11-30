package com.oracle.ugbu.ouai.ad.model

case class Configuration(inputPath : String,
                         outputPath : String,
                         configPath : String,
                         holidaysPath : String,
                         metadataPath : String,
                         mode: String,
                         source: String)
