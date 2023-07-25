# Databricks notebook source
# MAGIC %md
# MAGIC # Partitioning and Bucketing
# MAGIC To ensure efficiency, it is important to store data in a way that it makes it quicker to be accessed later. It is similar to indexing.
# MAGIC 
# MAGIC [Watch: why partitioning matters](https://youtu.be/F9QhzT_YTWw?t=879)
# MAGIC 
# MAGIC In this notebook, we will demonstrate the following:
# MAGIC 1. Writing data to multiple files via Coalescing and Repartitioning
# MAGIC 2. Partitioning by a label
# MAGIC 3. Bucketing data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC We'll start with a CSV file written into a DataFrame

# COMMAND ----------

file_path = 'dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv'

data = spark.read\
  .option('header', True)\
  .csv(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing data to multiple files

# COMMAND ----------

# MAGIC %md
# MAGIC With small amounts of data, it might be suitable to write all of your data to a single file. But with large amounts of data, it might make more sense to write data to multiple files. We can accomplish this by using two different functions:
# MAGIC 1. **Coalescing**: This operation merges all the partitions specified number of partitions ([Read more](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce))
# MAGIC 2. **Repartition**: This operation partitions the data into 'N' blocks based on a given column ([Read more](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by **coalaescing** our data into 2

# COMMAND ----------

data.coalesce(2).write.mode('Overwrite').csv(f'{working_directory}/coalesce_example')

# COMMAND ----------

# MAGIC %md
# MAGIC You should see that there are now exactly 2 partitions 

# COMMAND ----------

display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/coalesce_example')))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try **repartitioning** the same data into 9 (because it's a large, odd, ridiculous  number) partitions

# COMMAND ----------

data.repartition(9).write.mode('Overwrite').csv(f'{working_directory}/repartition_example')

# COMMAND ----------

# MAGIC %md
# MAGIC You can see in the display that there are now exactly **9 partitions** (csv files) written for this file

# COMMAND ----------

files_df = spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/repartition_example'))
display(files_df.filter(files_df.name.contains(".csv")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Observation 1
# MAGIC 
# MAGIC Running a repartition with repartition value of 9 will result in 9 tasks:
# MAGIC 
# MAGIC ![Jobs view](https://github.com/data-derp/small-exercises/blob/master/databricks-spark-certification/assets/repartition_9_result.png?raw=true)
# MAGIC 
# MAGIC 
# MAGIC ## Observation 2
# MAGIC 
# MAGIC Clicking into the `i` and into the `Event Timeline` will show how the work is distributed over 2 cores:
# MAGIC 
# MAGIC ![RepartitionDistributionOver2Cores](https://github.com/data-derp/small-exercises/blob/master/databricks-spark-certification/assets/repartition_9_event_timeline.png?raw=true)
# MAGIC 
# MAGIC 
# MAGIC ## Observation 3
# MAGIC 
# MAGIC ![Fellowship9](https://github.com/data-derp/small-exercises/blob/master/databricks-spark-certification/assets/fellowship_9.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data partitioned by label
# MAGIC Partitioning by label results in two directories, one for each label.
# MAGIC 
# MAGIC [Read more](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy)

# COMMAND ----------

dbutils.fs.rm(f'{working_directory}/partitioned_data', True)
data.select('Label').distinct().show()
data.write.partitionBy('Label').csv(f'{working_directory}/partitioned_data')
display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/partitioned_data')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from partitioned datasets
# MAGIC Partitioning allows us to read from a single partition (instead of all of the data)

# COMMAND ----------

read_from_partitioned = spark.read.option('header', True).csv(f'{working_directory}/partitioned_data/')
display(read_from_partitioned.filter('Label = "b"').select('*'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bucket data by columns
# MAGIC This pre-sorts the data while writing. The result is that there isn't any shuffling during query execution. Efficient!

# COMMAND ----------

plane_data_path = 'dbfs:/databricks-datasets/asa/planes/plane-data.csv'

plane_data = spark.read\
  .option('header', True)\
  .csv(plane_data_path)

# COMMAND ----------

plane_data_selected = plane_data.select(*[col for col in plane_data.columns if col not in ['year']], plane_data.year.cast('integer'))

# COMMAND ----------

# Create a unique table name from your current_user
import re
db = re.sub('[^A-Za-z0-9]+', '', current_user)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

table_name = "bucketing_example"

# Clean up tables by the the same name
dbutils.fs.rm(f'/user/hive/warehouse/{db}.db', True)
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# COMMAND ----------

numberOfBucket = 10
columnToBucketBy = 'year'

plane_data_selected.write.format('parquet').bucketBy(numberOfBucket, columnToBucketBy).saveAsTable(table_name)

# COMMAND ----------

dbutils.fs.ls(f'/user/hive/warehouse/{db}.db')

# COMMAND ----------

# MAGIC %md
# MAGIC Also, you can see your data by going to the **Data** tab on the left

# COMMAND ----------

# Finally, remove your table since we won't use it for the rest of the exercise
# In practice, you might keep these around for longer for others to query

dbutils.fs.rm(f'/user/hive/warehouse/{db}.db', True)
spark.sql(f"DROP TABLE {table_name}")
