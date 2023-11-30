package com.oracle.ugbu.ouai.ad.writers

import com.oracle.ugbu.ouai.ad.model.OutputRow
import org.apache.spark.sql.Dataset

object ObjectStorageWriter {

  def write(outputDF: Dataset[OutputRow], outputPath: String, mode: String): Unit = {
    val outputRawPath = outputPath + "/raw"
    if ("stream".equals(mode)) {
      val query = outputDF
        .writeStream
        .format("delta")
        .option("checkpointLocation", outputPath + "/_checkpoint")
        .outputMode("append")
        .start(outputRawPath)

      query.awaitTermination()
    }
    else {
      outputDF.write.format("delta").mode("append").save(outputRawPath)
    }
  }
}
