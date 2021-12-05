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
            description,image,name,prepTime,recipeYield,url, ingredients,\
            substring(datePublished,1,7) Year_Date from Raw_data WHERE cast(datePublished as date) <= current_date and (cookTime !='PT' and  prepTime !='PT')")

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
            df1 = df1.withColumn("cookTimeMin", time)
            df2 = df1.withColumn("prepTimeTimeMin", time1)

            logging.info("Daily data Tranformation time calculation completed")
            df2.createOrReplaceTempView("Hello_fresh_Temp")

            df3 = self.spark.sql("select DIFFUCLTY,avg(Total_time) as Avg_Total_Time from (select a.Total_time,CASE WHEN a.Total_time <=30 THEN 'EASY' WHEN a.Total_time > 30 AND a.Total_time <=60 THEN 'MEDIUM' ELSE 'HARD' END AS DIFFUCLTY from (select (cookTimeMin+prepTimeTimeMin) as Total_time from Hello_fresh_Temp where (cookTimeMin+prepTimeTimeMin) >0 and upper(ingredients) like '%BEEF%') a ) b group by DIFFUCLTY")
            df4 = self.spark.sql("select diffuclty,avg_total_time from raj.Hello_Fresh_Final")

            df3.createOrReplaceTempView("df3")
            df4.createOrReplaceTempView("df4")

            df5=self.spark.sql( "select a.DIFFUCLTY,(a.Avg_Total_Time+b.avg_total_time)/2 as Avg_Total_Time from df3 a join df4 b on a.DIFFUCLTY = b.DIFFUCLTY")

            return df5

        except Exception as exp1:
            logging.error("An error while transforming daily data " + str(exp1))
            sys.exit(1)