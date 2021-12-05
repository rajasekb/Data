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
        Raw_Table = os.environ.get('RAW_TABLE')

        try:
            self.spark.sql("use raj")
            df.write.format("orc").mode("append").saveAsTable(Raw_Table)

            return df
        except Exception as exp1:
            logging.error("An error while persisting raw data " + str(exp1))
            sys.exit(1)