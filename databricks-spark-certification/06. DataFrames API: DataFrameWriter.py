# Databricks notebook source
# MAGIC %md
# MAGIC 💡 _Run the following command to initalise data setup and library imports_

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

# MAGIC %run ./init_data

# COMMAND ----------

# DBTITLE 1,Write data to the “core” data formats (csv, json, jdbc, orc, parquet, text and tables)
# MAGIC %md
# MAGIC 1. Writing data into CSV
# MAGIC 2. Writing data into JSON
# MAGIC 3. Writing data in parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Writing data into CSV

# COMMAND ----------

#reading data 
file_path = 'dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv'
data = spark.read\
  .option('header', True)\
  .csv(file_path)

#writing data into csv
data.write.mode('Overwrite').csv(f'{working_directory}/atlas_csv')
dbutils.fs.ls(f'{working_directory}/atlas_csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Writing data into JSON

# COMMAND ----------

#reading data 
file_path = 'dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv'
data = spark.read\
  .option('header', True)\
  .csv(file_path)

#writing a json file
data.write.mode('Overwrite').json(f'{working_directory}/atlas_json')
dbutils.fs.ls(f'{working_directory}/atlas_json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Writing data into Parquet

# COMMAND ----------

#reading data 
file_path = 'dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv'
data = spark.read\
  .option('header', True)\
  .csv(file_path)

# writing to parquet
data.write.mode('Overwrite').parquet(f'{working_directory}/atlas_parquet')
dbutils.fs.ls(f'{working_directory}/atlas_parquet')

# COMMAND ----------

# DBTITLE 1,How to write a data source to 1 single file or N separate files
# MAGIC %md
# MAGIC 1. **Coalescing**: This operation merges all the partitions specified number of partitions 
# MAGIC 2. **Repartition**: This operation partitions the data into 'N' blocks based on a given column(optional)

# COMMAND ----------

#reading data 
file_path = 'dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv'
data = spark.read\
  .option('header', True)\
  .csv(file_path)
#coalescing
data.coalesce(1).write.mode('Overwrite').csv(f'{working_directory}/coalesce_example')

'''
You can see in the display that there are now exactly 1 patition written for this file
'''
dbutils.fs.ls(f'{working_directory}/coalesce_example')

# COMMAND ----------

# Reading data 

file_path = 'dbfs:/databricks-datasets/atlas_higgs/atlas_higgs.csv'
data = spark.read\
  .option('header', True)\
  .csv(file_path)
# Repartition the data into 9 (because it's a large, odd, ridiculous  number) partitions
data.repartition(9).write.mode('Overwrite').csv(f'{working_directory}/repartition_example')


# You can see in the display that there are now exactly 9 patitions (csv files) written for this file
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

# DBTITLE 1,How to write partitioned data
# MAGIC %md
# MAGIC Partitioning: To allow operations to happen in a distributed i.e Parallel way,  Spark breaks the data into chunks so known as partitions. Partitions can be user defined as well ! 

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Partitioning directly translates to how the data will be physically stored in disk_

# COMMAND ----------

data.select('Label').distinct().show()

# COMMAND ----------

# writing the data parititoned by label
dbutils.fs.rm(f'{working_directory}/partitioned_data', True)
data.write.partitionBy('Label').csv(f'{working_directory}/partitioned_data')
dbutils.fs.ls(f'{working_directory}/partitioned_data')

# COMMAND ----------

#Reading from partitioned datasets
read_from_partitioned = spark.read.option('header', True).csv(f'{working_directory}/partitioned_data/')
read_from_partitioned.filter('Label = "b"').select('*').show()

# COMMAND ----------

# DBTITLE 1,Overwriting existing files
'''
We will try writing over an existing file
'''

data.write.csv(f'{working_directory}/atlas_csv')

# COMMAND ----------

'''
Now, let us use the "Overwrite" mode
'''
data.write.mode('Overwrite').csv(f'{working_directory}/atlas_csv')

# COMMAND ----------

# DBTITLE 1,How to bucket data by a given set of columns
# MAGIC %md 
# MAGIC Bucketing is the operation that "pre-shuffles"/"pre-sort" the data while writing. The layout information is persisted !

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Efficient bucketing results in optimized joins ! If you bucket on the same column you are joining on then there is no shuffling(moving data around nodes) of data at query execution._

# COMMAND ----------

#reading the file
file_path = 'dbfs:/databricks-datasets/asa/planes/plane-data.csv'

data = spark.read\
  .option('header', True)\
  .csv(file_path)
data = data.select(*[col for col in data.columns if col not in ['year']], data.year.cast('integer'))

# COMMAND ----------



# COMMAND ----------

# Create a unique table name from your current_user
import re
db = re.sub('[^A-Za-z0-9]+', '', current_user)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

table_name = "wrangling_with_spark_6_bucket_data"

# Clean up tables by the the same name
dbutils.fs.rm(f'/user/hive/warehouse/{db}.db', True)
spark.sql(f"DROP TABLE {table_name}")

# COMMAND ----------

numberOfBucket = 10
columnToBucketBy = 'year'

data.write.format('parquet').bucketBy(numberOfBucket, columnToBucketBy).saveAsTable(table_name)

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

# COMMAND ----------

# DBTITLE 1,How to configure options for specific formats
# MAGIC %md
# MAGIC There are many options that can be configured while writing to specific formats. 
# MAGIC 
# MAGIC **Options in write mode** : **function** : **Supported format**
# MAGIC 
# MAGIC 1. Header: Takes boolean value to indicate if header to be included while writing or not : CSV
# MAGIC 2. sep: Input a characeter to be used as seperator :CSV
# MAGIC 3. nullValue: Declares what character represents a null value in the file :CSV
# MAGIC 4. nanValue: Declares what character represents a null value in the file : CSV
# MAGIC 5. codec : Declares what compression codec Spark should use to read or write the file : CSV, JSON, PARQUET
# MAGIC 6. timestampFormat : Any string that conforms to Java’s SimpleDataFormat : JSON
# MAGIC 7. dateFormat : Any string that conforms to Java’s SimpleDataFormat : JSON

# COMMAND ----------

file_path = 'dbfs:/databricks-datasets/asa/planes/plane-data.csv'

data = spark.read\
  .option('header', True)\
  .csv(file_path)



#save to bzip format
dbutils.fs.rm(f'{working_directory}/compression/bzip2', True)
data.write.format('parquet')\
  .option('codec', 'bzip2')\
  .save(f'{working_directory}/compression/bzip2')

# save to snappy format
dbutils.fs.rm(f'{working_directory}/compression/snappy', True)
data.write.format('parquet')\
  .save(f'{working_directory}compression/snappy')

#save to gzip
dbutils.fs.rm(f'{working_directory}/compression/gzip', True)
data.write.format('parquet')\
  .option('codec', 'gzip')\
  .save(f'{working_directory}/compression/gzip')

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [Repartition](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition)
# MAGIC 2. [Coalesce](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce)
# MAGIC 3. [partitionBy method of DataFrameWriter class](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy)
# MAGIC 4. [Partitioning and Bucketing](https://youtu.be/F9QhzT_YTWw?t=875)

# COMMAND ----------

