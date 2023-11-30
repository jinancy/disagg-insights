"""
Main script to run the insight.
"""
import logging
import logging.config
import sys
import traceback
from argparse import ArgumentParser, Namespace

from pyhocon import ConfigFactory
from pyhocon import ConfigList

from disagg_insights.utils.spark_session_utils import create_spark_session
from disagg_insights.constants import OCIConstants
from disagg_insights.scoring_job import ScoringJob


# Create the parser
def _create_parser() -> ArgumentParser:
    """
     Initializes an ArgumentParser with expected job params

     :rtype: ArgumentParser
     :return: an initialized ArgumentParser
    """
    parser = ArgumentParser(prog='Disagg Insights',
                            description='Generates disagg insights')
    parser.add_argument('--python-log-config',
                        type=str,
                        required=True,
                        help='python log config')
    parser.add_argument('--config-file-path',
                        type=str,
                        required=True,
                        help='the path to conf file for the SparkSession')
    parser.add_argument('--input-path',
                        type=str,
                        required=True,
                        help='the path to Delta folder where the data resides')
    parser.add_argument('--output-path',
                        type=str,
                        required=True,
                        help='the path to Delta folder where the data resides')
    parser.add_argument('--model-bucket-name',
                        type=str,
                        required=True,
                        help='the bucket name where the model resides')
    parser.add_argument('--model-object-name',
                        type=str,
                        required=True,
                        help='the absolute object name where the model resides including any prefixes')
    parser.add_argument('--mode',
                        type=str,
                        required=True,
                        help='Mode can be either stream or batch')
    parser.add_argument('--source',
                        type=str,
                        required=True,
                        help='source')
    # parser.add_argument('--verbose',
    #                     type=bool,
    #                     default=False,
    #                     required=False,
    #                     help='Set job verbosity level')
    return parser


def _parse(argv: list, parser: ArgumentParser) -> Namespace:
    # strip all args of leading and trailing whitespace before passing them to the parser
    argv_stripped = [arg.strip() if type(arg) is str else arg for arg in argv]
    args, _ = parser.parse_known_args(argv_stripped)
    return args


# def configure_spark_logging(verbose, spark_session):
#     """
#     Spark logging is incredibly verbose at the INFO level, so squelch it unless Spark verbose is requested.
#     :param verbose: set Spark log levels to INFO if True, WARN if False
#     :type verbose: bool
#     :type spark_session: SparkSession
#     :param spark_session: the spark session in which to configure Spark logging
#     :return: None; side-effect of setting log levels for Spark classes
#     """
#     java_logger = spark_session._jvm.org.apache.log4j
#     java_level = java_logger.Level.INFO if verbose else java_logger.Level.WARN
#     java_logger.LogManager.getLogger("org").setLevel(java_level)
#     java_logger.LogManager.getLogger("akka").setLevel(java_level)
#
#     python_level = logging.INFO if verbose else logging.WARN
#     logging.getLogger('pyspark').setLevel(python_level)


def _get_os_config(os_config_parsed: ConfigList) -> dict:
    config = {'tenancy': os_config_parsed.get_string(OCIConstants['TENANCY_ID']),
              'fingerprint': os_config_parsed.get_string(OCIConstants['FINGERPRINT']),
              'tenant-name': os_config_parsed.get_string(OCIConstants['NAMESPACE']),
              'key_file': os_config_parsed.get_string(OCIConstants['PEM_FILE_PATH']),
              'region': os_config_parsed.get_string(OCIConstants['REGION']),
              'user': os_config_parsed.get_string(OCIConstants['USER_ID'])}
    return config


def main(argv: list = None):
    parser = _create_parser()
    args = _parse(argv, parser)

    # Loading logging from the external log config
    logging.config.fileConfig(args.python_log_config)
    logger = logging.getLogger(__name__)
    logger.info("created logger from python log config")

    try:
        os_config_parsed = ConfigFactory.parse_file(args.config_file_path)
        logger.info("Parsing arguments completed")

        spark = create_spark_session(os_config_parsed)
        # configure_spark_logging(args.verbose, spark)
        logger.info("Spark session initialized")

        os_config = _get_os_config(os_config_parsed)
        job = ScoringJob(spark, args, os_config)
        job.run()
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(str(e))
        raise e


if __name__ == "__main__":
    main(sys.argv[1:])
