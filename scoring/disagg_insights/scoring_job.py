import logging
import os
from argparse import Namespace

from pyspark.sql import SparkSession

from disagg_insights.readers.feature_extraction_reader import FeatureExtractionReader
from disagg_insights.readers.model_reader import ModelReader
from disagg_insights.transformers.model_scorer import ModelScorer
from disagg_insights.transformers.post_processor import PostProcessor
from disagg_insights.writers.object_storage_writer import ObjectStorageWriter


class ScoringJob:

    def __init__(self, spark: SparkSession, args: Namespace, os_config: dict):
        self._spark = spark
        self._args = args
        self._os_config = os_config
        self._logger = logging.getLogger(__name__)

    def run(self):
        model_path = os.path.join(self._args.model_bucket_name, self._args.model_object_name)
        self._logger.info("Started reading model from bucket = {}".format(model_path))
        model_reader = ModelReader(self._os_config, self._args.model_bucket_name, self._args.model_object_name)
        model_in_bytes = model_reader.read()
        self._logger.info("Finished reading model from bucket = {}, bytes = {}".format(model_path, model_in_bytes))

        features_df = FeatureExtractionReader.read_features_df(self._spark, self._args.input_path, self._args.mode)
        scored_df = ModelScorer.predict_appliances_df(features_df, model_in_bytes)

        exploded_df = PostProcessor.explode_scored_dataset(scored_df)
        timed_df = PostProcessor.resolve_start_end_times(exploded_df)
        mapped_df = PostProcessor.map_appliance_id_to_appliance_name(timed_df)
        columns_dropped = PostProcessor.drop_columns(mapped_df)
        output_df = PostProcessor.drop_appliances(columns_dropped, self._args.source)
        ObjectStorageWriter.write(output_df, self._args.output_path, self._args.mode)

    # def run_coalesce(self):
    #     df = (self._spark.readStream
    #           .format("csv")
    #           .option("header", "true")
    #           .load(self._args.input_path))
    #     df_repartitioned = df.repartition()
    #     (
    #
    #     )


