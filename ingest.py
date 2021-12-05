from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import os
import logging
import sys

class Ingest:
    logging.basicConfig(level="INFO")


    #spark has init method which is like construtor using that we can intialize variables during object creation
    def __init__(self,spark):
        self.spark = spark

    def ingest_data(self):
        src_dir = os.environ.get('SRC_DIR')
        src_file_format = os.environ.get('SRC_FILE_FORMAT')

        try:
            customer_df = self.spark.read.format(src_file_format).load(src_dir)
            return customer_df

        except Exception as exp1:
            logging.error("An error while extracting raw data " + str(exp1))
            sys.exit(1)

    def Daily_data(self):
        #orders_src_dir = os.environ.get('ORDERS_SRC_DIR')
        #orders_src_file_format = os.environ.get('ORDERS_SRC_FILE_FORMAT')

        try:
            daily_df = self.spark.sql("select * from raj.Hello_Fresh")
            return daily_df

        except Exception as exp1:
            logging.error("An error while extracting daily data " + str(exp1))
            sys.exit(1)



