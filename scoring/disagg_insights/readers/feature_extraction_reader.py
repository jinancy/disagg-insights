import logging
import time


class FeatureExtractionReader:
    logger = logging.getLogger(__name__)

    @staticmethod
    def read_features_df(spark, input_path, mode):
        if mode == "stream":
            FeatureExtractionReader.is_path_exist(spark, input_path)
            return (spark.readStream
                    .option('maxFilesPerTrigger', '1')
                    .option('ignoreChanges', 'true')
                    .format("delta")
                    .load(input_path)
                    .repartition(10)
                    )
        else:
            return spark.read.format("delta").load(input_path)

    @staticmethod
    def is_path_exist(spark, input_path):
        print("Checking output path presence {}".format(input_path))
        while True:
            try:
                spark.read.format("delta").load(input_path)
                print("{} path is available now".format(input_path))
                break
            except Exception as e:
                print(e)
                time.sleep(60)




