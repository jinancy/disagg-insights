
import logging

from pyspark.sql import DataFrame


class ObjectStorageWriter:
    logger = logging.getLogger(__name__)

    @staticmethod
    def write(output_df, output_path, mode):
        data_output_path = output_path + "/raw"
        if mode == "stream":
            query = (output_df
                     .writeStream
                     .format("csv")
                     .option("header", "true")
                     .option("checkpointLocation", output_path + "/_checkpoint")
                     .outputMode("append")
                     .start(data_output_path)
                     )
            query.awaitTermination()

        else:
            output_df.write.format("csv").option("header", "true").mode("append").save(data_output_path)
