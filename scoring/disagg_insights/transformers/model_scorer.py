import pandas as pd
import numpy as np
from pyspark.sql.types import *
from pyspark.sql.functions import *
from disagg_insights.readers.model_reader import ModelReader


class ModelScorer:

    @staticmethod
    def predict_appliances_df(spark_df, model_in_bytes: bytes):
        add_predictions = udf(lambda z: ModelScorer.predict_appliances(z, model_in_bytes),
                              ArrayType(ArrayType(FloatType())))
        return spark_df.withColumn("appliance_predictions", add_predictions("feature_vector"))

    @staticmethod
    def predict_appliances(feature_vectors, model_in_bytes: bytes):
        model = ModelReader.load_model_api(model_in_bytes)
        predictions_3d = model.predict(np.array([pd.DataFrame(feature_vectors)]))
        predictions_2d = predictions_3d[0]
        return predictions_2d.tolist()
