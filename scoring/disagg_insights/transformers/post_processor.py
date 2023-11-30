from pyspark.sql.functions import col, unix_timestamp, when, udf
from pyspark.sql.types import *


class PostProcessor:

    @staticmethod
    def explode_scored_dataset(df_scored):
        """
        Given an input dataset in the SCORED_SCHEMA, explode it such that there is one row for each
        utility-servicepoint-timestep-appliance combination.

        Example - input:

        | utility_code | service_point_id | date         | day_start_time | yhat                                     |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | [[0.10, 0.11, 0.12], [0.20, 0.21, 0.22]] |

        output:

        | utility_code | service_point_id | date         | day_start_time | timestep | appliance_id | usage |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | 0        | 0            | 0.10  |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | 0        | 1            | 0.11  |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | 0        | 2            | 0.12  |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | 1        | 0            | 0.20  |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | 1        | 1            | 0.21  |
        | 'util'       | '1'              | '2019-01-01' | <timestamp>    | 1        | 2            | 0.22  |

        :param df_scored: the scored dataframe
        :type df_scored: pyspark.sql.DataFrame

        :return: the exploded dataframe
        :rtype: pyspark.sql.DataFrame
        """
        usages_col = 'appliance_usages'
        df_one_row_per_time = (
            df_scored.selectExpr("*", "posexplode({appliance_predictions}) as ({timestep}, {appliance_usages})"
                                 .format(appliance_predictions="appliance_predictions",
                                         timestep="timestep",
                                         appliance_usages=usages_col))
        )
        appliance_id_col = 'appliance_id'
        df_one_row_per_appliance = (
            df_one_row_per_time.selectExpr("*", "posexplode({appliance_usages}) as ({appliance_id}, {usage})"
                                           .format(appliance_usages=usages_col,
                                                   appliance_id=appliance_id_col,
                                                   usage="usage"))
        )
        return df_one_row_per_appliance

    @staticmethod
    def resolve_start_end_times(df_exploded):
        """
        Given an exploded dataframe with columns DAY_START_TIME and TIMESTEP, returns a dataframe with appropriate start
        and end times in each row in START_TIME and END_TIME columns

        :param df_exploded: the exploded dataframe
        :type df_exploded: pyspark.sql.DataFrame

        :return: a dataframe with START_TIME and END_TIME columns
        :rtype: pyspark.sql.DataFrame
        """
        return (df_exploded.withColumn("new_local_15min",
                                       (unix_timestamp("local_15min") +
                                        (60 * (col("timestep") * col("sliding_window")))).cast("timestamp"))
                )

    @staticmethod
    def map_appliance_id_to_appliance_name(df_to_map):
        """
        Add a column with the appliance name for each row in the input dataset.

        :param df_to_map: the dataframe to add mappings for. Must contain APPLIANCE_ID column.
        :type df_to_map: pyspark.sql.DataFrame

        :return: A copy of the original dataframe with an additional APPLIANCE column.
        :rtype: pyspark.sql.DataFrame
        """
        appliance_udf = udf(lambda appliance_id: PostProcessor.get_appliance_name(appliance_id), StringType())
        return df_to_map.withColumn('appliance', appliance_udf(col('appliance_id')))

    @staticmethod
    def get_appliance_name(appliance_id) -> str:
        # TODO: read from the model metadata
        return ["air_condition", "dishwasher", "dryer", "ev", "furnace",
                "oven", "pool_pump", "refrigerator", "washer", "water_heater"][appliance_id]


    @staticmethod
    def drop_columns(mapped_df):
        return (mapped_df
                .drop("local_15min", "appliance_id", "sliding_window", "feature_vector",
                      "appliance_predictions", "timestep", "appliance_usages")
                .withColumnRenamed("new_local_15min", "time")
                .withColumnRenamed("appliance", "appliance_name")
                .withColumn("usage", col("usage").cast("double"))
                .withColumn("usage", when(((col("usage").isNull()) | (col("usage") < 0.00)),
                                          0.00).otherwise(col("usage")))
                .select("dataid", "time", "appliance_name", "usage", "city", "state"))

    @staticmethod
    def drop_appliances(df, source):
        if "innovation_lab" == source:
            drop_appliances = ["air_condition", "dishwasher", "dryer", "ev", "furnace",
                               "oven", "pool_pump", "washer"]
        else:
            drop_appliances = ["pool_pump"]
        return df.filter(~col('appliance_name').isin(drop_appliances))

