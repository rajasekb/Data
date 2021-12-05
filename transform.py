from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType
import os
import logging
import sys

class Transform:
    logging.basicConfig(level="INFO")

    def __init__(self,spark):
        self.spark=spark

    def raw_transform_data(self,df):
        logging.info("Raw data Tranformation started")
        try:
            df.createOrReplaceTempView("Raw_data")
            df1 = self.spark.sql("select cookTime,cast(datePublished as date),\
            description,image,name,prepTime,recipeYield,url,split(ingredients,'\n') as ingredients,\
            substring(datePublished,1,7) Year_Date from Raw_data WHERE cast(datePublished as date) <= current_date ")

            return df1
        except Exception as exp1:
            logging.error("An error while transforming raw data " + str(exp1))
            sys.exit(1)

    def orders_transform_data(self,df1):
        logging.info("Data Tranformation started")

        try:
            df1.createOrReplaceTempView("orders_tempview")
            df2 = self.spark.sql("select order_prod_id from orders_tempview")
            return df2
        except Exception as exp1:
            logging.error("An error while transforming the data " + str(exp1))
            sys.exit(1)