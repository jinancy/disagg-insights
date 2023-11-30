import logging.config
import time
import traceback
import unittest
from argparse import Namespace

from pyhocon import ConfigFactory
from pyspark.sql import SparkSession

from disagg_insights.main import _get_os_config
from disagg_insights.readers.feature_extraction_reader import FeatureExtractionReader
from disagg_insights.scoring_job import ScoringJob
from disagg_insights.transformers.model_scorer import ModelScorer
from disagg_insights.transformers.post_processor import PostProcessor
from disagg_insights.utils.spark_session_utils import create_spark_session
from disagg_insights.writers.object_storage_writer import ObjectStorageWriter


class TestMain(unittest.TestCase):
    logger = logging.getLogger(__name__)
    oci_config_path = "/Users/siva.tetala/ws/disagg-insights/scoring/tests/resources/os.params.conf"

    # def test_oci(self):
    #     try:
    #         os_config_parsed = ConfigFactory.parse_file(self.oci_config_path)
    #         spark = create_spark_session(os_config_parsed)
    #         self.logger.info("Spark session initialized")
    #         os_config = _get_os_config(os_config_parsed)
    #         args = Namespace(model_bucket_name='cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1',
    #                          model_object_name='innovation_lab_disagg/model/model.h5',
    #                          input_path='oci://cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1@oraclegbudevcorp:/innovation_lab_disagg/pre-process/5/raw',
    #                          mode='batch',
    #                          source = 'innovation_lab',
    #                          output_path='oci://cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1@oraclegbudevcorp:/innovation_lab_disagg/scorer/5')
    #         job = ScoringJob(spark, args, os_config)
    #         start = time.time()
    #         print('Start Time : ' + str(start))
    #         job.run()
    #         end = time.time()
    #         print('End Time : ' + str(end))
    #         sec = end - start
    #         print('Time Elapsed : ' + str(sec))
    #     except Exception as e:
    #         self.logger.error(traceback.format_exc())
    #         self.logger.error(str(e))
    #         raise e

    def test_oci_pecan(self):
        try:
            os_config_parsed = ConfigFactory.parse_file(self.oci_config_path)
            spark = create_spark_session(os_config_parsed)
            self.logger.info("Spark session initialized")
            os_config = _get_os_config(os_config_parsed)
            args = Namespace(model_bucket_name='cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1',
                             model_object_name='innovation_lab_disagg/model/model.h5',
                             input_path='oci://cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1@oraclegbudevcorp:/pecan_st_disagg/demo/pre-process/4/raw',
                             mode='stream',
                             source = 'pecan',
                             output_path='oci://cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1@oraclegbudevcorp:/appliance_disagg/pecan_st/scorer_insights')
                             # output_path='oci://cds_gbua_cndevcorp_ugbu_ouai_njuakanevizj5m0dxq9d_1@oraclegbudevcorp:/pecan_st_disagg/demo/scorer/4')
            job = ScoringJob(spark, args, os_config)
            start = time.time()
            print('Start Time : ' + str(start))
            job.run()
            end = time.time()
            print('End Time : ' + str(end))
            sec = end - start
            print('Time Elapsed : ' + str(sec))
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(str(e))
            raise e

    # def test_local(self):
    #     spark = SparkSession.builder.master("local[*]").getOrCreate()
    #     spark.sparkContext.setLogLevel("warn")
    #     mode = 'batch'
    #     source = 'innovation_lab'
    #     input_path = 'file:///Users/siva.tetala/ws/disagg/innovation_lab/pre-process/innovation_lab_disagg/pre-process/7/raw'
    #     output_path = 'file:///Users/siva.tetala/ws/disagg/innovation_lab/scorer/7'
    #     try:
    #         start = time.time()
    #         print('Start Time : ' + str(start))
    #         model_in_bytes = open('/Users/siva.tetala/ws/models/2.50.0/model.h5', 'rb').read()
    #         features_df = FeatureExtractionReader.read_features_df(spark, input_path, mode)
    #         features_df = features_df.repartition(4)
    #         scored_df = ModelScorer.predict_appliances_df(features_df, model_in_bytes)
    #         exploded_df = PostProcessor.explode_scored_dataset(scored_df)
    #         timed_df = PostProcessor.resolve_start_end_times(exploded_df)
    #         mapped_df = PostProcessor.map_appliance_id_to_appliance_name(spark, timed_df)
    #         columns_dropped = PostProcessor.drop_columns(mapped_df)
    #         output_df = PostProcessor.drop_appliances(columns_dropped, source)
    #         ObjectStorageWriter.write(output_df, output_path, mode)
    #         end = time.time()
    #         print('End Time : ' + str(end))
    #         sec = end - start
    #         print('Time Elapsed : ' + str(sec))
    #     except Exception as e:
    #         self.logger.error(traceback.format_exc())
    #         self.logger.error(str(e))
    #         raise e

    # def test_local_pecan(self):
    #     spark = SparkSession.builder.master("local[*]").getOrCreate()
    #     spark.sparkContext.setLogLevel("warn")
    #     mode = 'stream'
    #     source = 'pecan'
    #     # input_path = 'file:///Users/siva.tetala/ws/disagg/pecan_st_disagg/pre-process/1/raw_partitioned'
    #     # output_path = 'file:///Users/siva.tetala/ws/disagg/pecan_st_disagg/scorer/1'
    #     input_path = 'file:///Users/siva.tetala/ws/disagg/pecan_st_disagg/pre-process/batch_unique/1/raw'
    #     output_path = 'file:///Users/siva.tetala/ws/disagg/pecan_st_disagg/scorer/batch_unique/1'
    #     try:
    #         start = time.time()
    #         print('Start Time : ' + str(start))
    #         model_in_bytes = open('/Users/siva.tetala/ws/models/2.50.0/model.h5', 'rb').read()
    #         features_df = FeatureExtractionReader.read_features_df(spark, input_path, mode)
    #         features_df = features_df.repartition(4)
    #         scored_df = ModelScorer.predict_appliances_df(features_df, model_in_bytes)
    #         exploded_df = PostProcessor.explode_scored_dataset(scored_df)
    #         timed_df = PostProcessor.resolve_start_end_times(exploded_df)
    #         mapped_df = PostProcessor.map_appliance_id_to_appliance_name(spark, timed_df)
    #         columns_dropped = PostProcessor.drop_columns(mapped_df)
    #         output_df = PostProcessor.drop_appliances(columns_dropped, source)
    #         ObjectStorageWriter.write(output_df, output_path, mode)
    #         end = time.time()
    #         print('End Time : ' + str(end))
    #         sec = end - start
    #         print('Time Elapsed : ' + str(sec))
    #     except Exception as e:
    #         self.logger.error(traceback.format_exc())
    #         self.logger.error(str(e))
    #         raise e

    # def test_local_pecan_func(self):
    #     spark = SparkSession.builder.master("local[*]").getOrCreate()
    #     spark.sparkContext.setLogLevel("warn")
    #     mode = 'stream'
    #     source = 'pecan'
    #     input_path = 'file:///Users/siva.tetala/ws/disagg/pecan_st_disagg/func_test/2/pre-process/raw'
    #     output_path = 'file:///Users/siva.tetala/ws/disagg/pecan_st_disagg/func_test/2/scorer'
    #     try:
    #         start = time.time()
    #         print('Start Time : ' + str(start))
    #         model_in_bytes = open('/Users/siva.tetala/ws/models/2.50.0/model.h5', 'rb').read()
    #         features_df = FeatureExtractionReader.read_features_df(spark, input_path, mode)
    #         features_df = features_df.repartition(4)
    #         scored_df = ModelScorer.predict_appliances_df(features_df, model_in_bytes)
    #         exploded_df = PostProcessor.explode_scored_dataset(scored_df)
    #         timed_df = PostProcessor.resolve_start_end_times(exploded_df)
    #         mapped_df = PostProcessor.map_appliance_id_to_appliance_name(timed_df)
    #         columns_dropped = PostProcessor.drop_columns(mapped_df)
    #         output_df = PostProcessor.drop_appliances(columns_dropped, source)
    #         ObjectStorageWriter.write(output_df, output_path, mode)
    #         end = time.time()
    #         print('End Time : ' + str(end))
    #         sec = end - start
    #         print('Time Elapsed : ' + str(sec))
    #     except Exception as e:
    #         self.logger.error(traceback.format_exc())
    #         self.logger.error(str(e))
    #         raise e


if __name__ == '__main__':
    unittest.main()