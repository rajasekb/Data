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

    def daily_transform_data(self,df1):
        logging.info("Daily data Tranformation started")

        try:
            cook_time = F.col("cookTime")
            prep_Time = F.col("prepTime")
            h1 = F.regexp_extract(cook_time, 'PT([0-9]*)H([0-9]*)M', 1).cast("int")
            m1 = F.regexp_extract(cook_time, 'PT([0-9]*)H([0-9]*)M', 2).cast("int")
            h2 = F.regexp_extract(cook_time, 'PT([0-9]*)H$', 1).cast("int")
            m2 = F.regexp_extract(cook_time, 'PT([0-9]*)M', 1).cast("int")

            time = F.when(h1.isNotNull() & m1.isNotNull(), h1 * 60 + m1) \
                .when(h2.isNotNull(), h2 * 60) \
                .when(m2.isNotNull(), m2) \
                .otherwise(0)

            h1 = F.regexp_extract(prep_Time, 'PT([0-9]*)H([0-9]*)M', 1).cast("int")
            m1 = F.regexp_extract(prep_Time, 'PT([0-9]*)H([0-9]*)M', 2).cast("int")
            h2 = F.regexp_extract(prep_Time, 'PT([0-9]*)H$', 1).cast("int")
            m2 = F.regexp_extract(prep_Time, 'PT([0-9]*)M', 1).cast("int")

            time1 = F.when(h1.isNotNull() & m1.isNotNull(), h1 * 60 + m1) \
                .when(h2.isNotNull(), h2 * 60) \
                .when(m2.isNotNull(), m2) \
                .otherwise(0)
            time = time + time1
            df2 = df1.withColumn("cookTimeMin", time)

            logging.info("Daily data Tranformation time calculation completed")

            difficulty = F.when(time < 30, "easy") \
                .when((time >= 30) & (time <= 60), "medium") \
                .otherwise("hard")
            logging.info("Daily data Tranformation time Issue query started")

            df3 = df2.filter(F.lower(F.col("ingredients")).rlike("beef")).withColumn("difficulty", difficulty).groupBy("difficulty").agg(F.avg("cookTimeMin").alias("avgTime")).show()

            return df3

        except Exception as exp1:
            logging.error("An error while transforming daily data " + str(exp1))
            sys.exit(1)