import unittest
import transform
from pyspark.sql import SparkSession

class TransformTest(unittest.TestCase):

        def test_transform_value(self):

                spark = SparkSession.builder \
                        .appName("testing app") \
                        .enableHiveSupport().getOrCreate()


                df = spark.read.json("sample_data.csv")
                df.show()

                transform_process = transform.Transform(spark)
                tranformed_df = transform_process.raw_transform_data(df)
                tranformed_df.show()
                count1 = tranformed_df.count()
                PT_time = tranformed_df.filter("prepTime = 'PT1M'").select("recipeYield").collect()[0].recipeYield
                print("Record count is " +str(count1))
                print("Record count is " + str(PT_time))
                #self.assertEqual(3,count1)
                self.assertEqual(4,int(PT_time))

        def test_second_name(self):
                self.assertTrue('PYTHON'.isupper())




if __name__ == '__main__':
        unittest.main()
