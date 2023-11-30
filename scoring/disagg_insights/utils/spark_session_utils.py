"""
spark_session_utils.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""
from pyhocon import ConfigList
from pyspark.sql import SparkSession

from disagg_insights.constants import OCIConstants


def create_spark_session(os_config_parsed: ConfigList) -> SparkSession:
    """
    :param os_config_parsed:
    :return:
    """
    spark_ssn = (SparkSession.builder
                 .appName("SpeedRacer")
                 .config("fs.oci.client.hostname",
                         os_config_parsed.get_string(OCIConstants['OS_HOSTNAME']))
                 .config("fs.oci.client.auth.tenantId",
                         os_config_parsed.get_string(OCIConstants['TENANCY_ID']))
                 .config("fs.oci.client.auth.userId",
                         os_config_parsed.get_string(OCIConstants['USER_ID']))
                 .config("fs.oci.client.auth.fingerprint",
                         os_config_parsed.get_string(OCIConstants['FINGERPRINT']))
                 .config("fs.oci.client.auth.pemfilepath",
                         os_config_parsed.get_string(OCIConstants['PEM_FILE_PATH']))
                 .config("fs.bmc.impl",
                         "com.oracle.bmc.hdfs.BmcFilesystem")
                 # .config("spark.delta.logStore.class",
                 #         "org.apache.spark.sql.delta.storage.OciSingleDriverLogStore")
                 # when running on local only
                 .config("spark.delta.logStore.oci.impl",
                         "io.delta.storage.OracleCloudLogStore")
                 .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
                 # when running on local only
                 # .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                 .config("spark.sql.broadcastTimeout", "10000")
                 .getOrCreate())

    spark_ssn.sparkContext._jsc.hadoopConfiguration().set(
        "fs.oci.client.hostname", os_config_parsed.get_string(OCIConstants['OS_HOSTNAME']))

    spark_ssn.sparkContext._jsc.hadoopConfiguration().set(
        "fs.oci.client.auth.tenantId", os_config_parsed.get_string(OCIConstants['TENANCY_ID']))

    spark_ssn.sparkContext._jsc.hadoopConfiguration().set(
        "fs.oci.client.auth.userId", os_config_parsed.get_string(OCIConstants['USER_ID']))

    spark_ssn.sparkContext._jsc.hadoopConfiguration().set(
        "fs.oci.client.auth.fingerprint", os_config_parsed.get_string(OCIConstants['FINGERPRINT']))

    spark_ssn.sparkContext._jsc.hadoopConfiguration().set(
        "fs.oci.client.auth.pemfilepath", os_config_parsed.get_string(OCIConstants['PEM_FILE_PATH']))

    spark_ssn.sparkContext._jsc.hadoopConfiguration().set(
        "namespace", os_config_parsed.get_string(OCIConstants['NAMESPACE']))

    # uncomment when running on cluster
    # (spark_ssn.sparkContext.getConf().set(
    #     "spark.sql.shuffle.partitions",
    #     ([int(spark_ssn.conf.get("spark.executor.instances"))
    #       if 'spark.executor.instances' in
    #          [i[0] for i in spark_ssn.sparkContext.getConf().getAll()] else 1][0] *
    #      [int(spark_ssn.conf.get("spark.kubernetes.executor.limit.cores"))
    #       if 'spark.kubernetes.executor.limit.cores' in
    #          [i[0] for i in spark_ssn.sparkContext.getConf().getAll()] else 1][0]
    #      )
    # ))

    spark_ssn.sparkContext.setLogLevel("error")
    return spark_ssn
