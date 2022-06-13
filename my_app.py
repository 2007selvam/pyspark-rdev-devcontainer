#!/usr/bin/env python3
__author__ = 'brad'
"""
My PySpark client
"""

import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import current_timestamp, input_file_name


spark = SparkSession.builder.appName('com.dadoverflow.mypysparkclient').getOrCreate()
log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)


def build_dataframe(data):
    """Simple function to build a dataframe"""
    df = spark.createDataFrame(data, ['fname', 'lname', 'age'])
    return df


def main(argv):
    LOGGER.info('Starting application')
    some_data = [('Homer', 'Simpson', 35), ('Marge', 'Simpson', 32)]
    build_dataframe(some_data)
    LOGGER.info('Completing application')


if __name__ == "__main__":
    main(sys.argv[1:])
