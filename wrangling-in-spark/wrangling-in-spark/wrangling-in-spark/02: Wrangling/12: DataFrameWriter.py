# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrameWriter
# MAGIC Once you have completed some queries and have a DataFrame with your result, you will want to write that data and save it somewhere.
# MAGIC 
# MAGIC In this exercise, we'll do the following:
# MAGIC 1. Write data into the CSV format
# MAGIC 2. Write data into the JSON format
# MAGIC 3. Write data into the parquet format
# MAGIC 4. Overwriting existing data
# MAGIC 5. Writing with options

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
# MAGIC ## Write data into the CSV format

# COMMAND ----------

data.write.mode('Overwrite').csv(f'{working_directory}/atlas_csv')

display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/atlas_csv')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data into the JSON format

# COMMAND ----------

data.write.mode('Overwrite').json(f'{working_directory}/atlas_json')

display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/atlas_json')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data into the Parquet format

# COMMAND ----------

data.write.mode('Overwrite').parquet(f'{working_directory}/atlas_parquet')

display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/atlas_parquet')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overwrite existing files

# COMMAND ----------

# DBTITLE 0,Overwriting existing files
data.write.csv(f'{working_directory}/atlas_csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Uh oh, what happened? It seems like the directory `atlas_csv` already exists. We can use the `Overwrite` mode to prevent this error.

# COMMAND ----------

data.write.mode('Overwrite').csv(f'{working_directory}/atlas_csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing with options
# MAGIC 
# MAGIC There are many options that can be configured while writing data to specific formats. Here are some examples.
# MAGIC 
# MAGIC | Option | Description | Supported Data Format | Scope |
# MAGIC | --- | --- | --- | --- |
# MAGIC | header | boolean to indicate if a header exists | csv | read/write |
# MAGIC | sep | input character as separator for each field and value | csv | read/write |
# MAGIC | lineSep | input character as line separator | csv, json | read/write |
# MAGIC | nullValue | what written character represents a null value in the dataset | csv | read/write |
# MAGIC | nanValue | what written character represents a nan value in the dataset | csv | read |
# MAGIC | compression | what compression codec Spark should use to write a file <br> (none, bzip2, gzip, lz4, snappy and deflate) | csv, json, text | write |
# MAGIC | compression | what compression codec Spark should use to write a file <br> (none, uncompressed, snappy, gzip, lzo, brotli, lz4, and zstd) | parquet | write |
# MAGIC | timestampFormat | how to format a timestamp | csv, json | read/write |
# MAGIC | dateformat | how to format a date | csv, json | read/write |
# MAGIC 
# MAGIC For more options, refer to following.
# MAGIC * [CSV data source options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option)
# MAGIC * [JSON data source options](https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option)
# MAGIC * [Parquet data source options](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to CSV format with bzip2 compression

# COMMAND ----------

dbutils.fs.rm(f'{working_directory}/compression/bzip2', True)

# This creates snappy.parquet as 'codec' is not an supported option
data.write.format('parquet')\
  .option('codec', 'bzip2')\
  .save(f'{working_directory}/compression/bzip2/codec-parquet')
display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/compression/bzip2/codec-parquet')))

# This hit IllegalArgumentException as codec 'bzip2' is not available for parquet
# data.write.format('parquet')\
#   .option('compression', 'bzip2')\
#   .save(f'{working_directory}/compression/bzip2/compression-parquet')
# display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/compression/bzip2/compression-parquet')))

# This should works
data.write.format('csv')\
  .option('compression', 'bzip2')\
  .save(f'{working_directory}/compression/bzip2/compression-csv')
display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/compression/bzip2/compression-csv')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to gzip format

# COMMAND ----------

dbutils.fs.rm(f'{working_directory}/compression/gzip', True)
data.write.format('parquet')\
  .option('compression', 'gzip')\
  .save(f'{working_directory}/compression/gzip')

display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/compression/gzip')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Snappy format

# COMMAND ----------

dbutils.fs.rm(f'{working_directory}/compression/snappy', True)
data.write.format('parquet')\
  .save(f'{working_directory}/compression/snappy')

display(spark.createDataFrame(dbutils.fs.ls(f'{working_directory}/compression/snappy')))