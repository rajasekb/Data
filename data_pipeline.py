from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType
import ingest
import transform
import persist
import os
import logging



class Pipeline:
    logging.basicConfig(level="INFO")

    def run_pipeline(self):
        # self is used because this is class stable method.Self is used to represent the instance of the class.Using self we can access class level variables
        print("Running Pipeline")
        # import the file by specifiying file name and to instinate give the file name and class name


        ingest_process = ingest.Ingest(self.spark)
        logging.info("Raw Data extraction started")
        ingest_process.ingest_data()
        df = ingest_process.ingest_data()
        logging.info("Raw Data extraction completed")

        transform_process = transform.Transform(self.spark)
        transformed_df = transform_process.raw_transform_data(df)
        transformed_df.show(2,truncate=False)
        logging.info("Raw Data Transformation completed")


        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(transformed_df)
        logging.info("Raw Data persisit completed")


        ingest_process_orders = ingest.Ingest(self.spark)
        logging.info("Orders Data Ingestion started")
        ingest_process_orders.orders_data()
        df1 = ingest_process_orders.orders_data()
        df1.show()
        logging.info("Orders Data Ingestion completed")

        orders_transform_process = transform.Transform(self.spark)
        logging.info("Orders Data transformation started")
        orders_transformed_df = orders_transform_process.orders_transform_data(df1)
        orders_transformed_df.show(10,truncate=False)
        logging.info("Orders Data Transformation completed")

        return

    def create_spark_session(self):
        self.spark = SparkSession.builder.appName("Hello_Fresh").enableHiveSupport().getOrCreate()
        self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


if __name__ == '__main__':
    #Instanting the Pipeline class for invoking
    pipeline = Pipeline()
    # Calling run_pipeline using Instance of the class
    pipeline.create_spark_session()
    logging.info("Spark Session created")
    pipeline.run_pipeline()

