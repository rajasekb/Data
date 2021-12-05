# Hello Fresh Assignment

## My Understanding:

Data from Kafka events will be received and this has to be stored in S3 bucket and from then we need to calculate the difficulty level based on preparation time and cooking time and finally output should be stored as CSV and it should have two columns difficulty level and average preparation and cooking time.At any given point the file output file should have three rows i.e easy,medium and hard and there respective average preparation and cook time

## Implementation:
###Task 1:
I used spark to read the JSON files from standard location based on pattern.
As part of cleansing I fileterd out the records which has only "PT" for cook and prep time and also picked the records which has date published less than or equal to current date
I tried converting ingredients into array by splitting it but I was facing issue while filtering records in subsequent steps and hence I did not include it final code
Finally I loaded the data into HIVE table ( which uses HDFS as storage). The reason for creating HIVE table is that in further steps we can retrieve only required data based on dates
I also partitioned table based on Year-Month as we are receiving data daily and we have huge historical data so creating partition on date will create overburden on Metastore
I determined no.of partitions of processed dataframe and that count I used writing in coalsece function
Same can be implemented by loading into S3 bucket as well

###Task2 :
Read the data from Historical raw table and pull only latest records based on dates
As cooktime and preptime are in ISO format , I used python regular expression to determine the minutes 
Based on that I calculated total_cook_time and derived difficulty level. Calculated average total cooking time for incoming data and this data is again used to calculate the overall total_cook_time by adding to data available in final table( this will not have any impact during first run and from second run onwards overall average total_cook_time will be available in final table)
the final output is again loaded into HIVE table with CSV format


Python files :
I created four python files as mentioned below
1. data_pipeline ---> This is the main file where I defined class and main method and will be calling all other files in this file based on functions
2. ingest ---> All data ingestion will happen in this file
3. transform ---> All the data transformation happens in this file
4. persist ---> All the loading data happens in this file
5. test_transformer ---> Created unit test case for one of the function which is available in transformer file
6. sample_data.csv ---> Sample data which is used for test cases
7. Hello_Fresh_Shell_Script ---> This script has all the environment variables and spark submit command which is used to run the program.

I handled exception in all the python files
I also logged messages wherever necessary 
This code is tested by exporting to cluster and it is working
This code can be scheduled in Airflow as it has resilient feature (auto restart) and also provides extra logging mechanism
This code can be integrated with looper so that code can be automatically migrated to higher enviornments
we can use dynamic allocation of resources during spark submit so that the resources which are available in cluster will be utilized
