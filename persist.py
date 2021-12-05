from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import os
import logging
import sys


class Persist:
    logging.basicConfig(level="INFO")

    def __init__(self,spark):
        self.spark=spark

    def persist_data(self,df):
        logging.info("Raw data persist started")
        Raw_schema = os.environ.get('RAW_SCHEMA')
        Raw_table = os.environ.get('RAW_TABLE')

        try:
            df.write.format("orc").mode("append").insertInto(Raw_schema+'.'+Raw_table)

            return df
        except Exception as exp1:
            logging.error("An error while persisting raw data " + str(exp1))
            sys.exit(1)

    def Daily_data_persist(self,df):
        logging.info("Daily data persist started")
        Pubhlish_schema = os.environ.get('PUBHLISH_SCHEMA')
        pubhlish_table = os.environ.get('PUBHLISH_TABLE')

        try:
            df.write.format("csv").mode("overwrite").insertInto(Pubhlish_schema+'.'+pubhlish_table)

            return df
        except Exception as exp1:
            logging.error("An error while persisting daily data " + str(exp1))
            sys.exit(1)