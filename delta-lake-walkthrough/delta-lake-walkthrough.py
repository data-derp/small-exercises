# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Introduction to Delta Lake
# MAGIC 
# MAGIC This notebook is a modified version of [this awesome demo](https://databricks.com/de/notebooks/Demo_Hub-Delta_Lake_Notebook.html) from Databricks 
# MAGIC 
# MAGIC ### Unifying Batch and Streaming Processing
# MAGIC 
# MAGIC ### Bringing ACID to Spark

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well. -->
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 7.4 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install wget

# COMMAND ----------

'''
Clear out existing working directory
'''
current_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]
working_directory=f"/FileStore/{current_user}/deltaDemo"
dbutils.fs.rm(working_directory, True)
dbutils.fs.mkdirs(working_directory)

# COMMAND ----------

import re
db = db = re.sub('[^A-Za-z0-9]+', '', current_user)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")


# COMMAND ----------

import random
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *


my_checkpoint_dir = "{working_directory}/chkpt/%s" % str(random.randint(0, 10000))

# User-defined function to generate random state
@udf(returnType=StringType())
def random_state():
  return str(random.choice(["CA", "TX", "NY", "WA"]))

# Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream(table_format, table_name, schema_ok=False, workload_source="structured-streaming"):
  """
  We're going to generate our own data from scratch using a simple "Rate Source".
  A Rate source generates data at the specified number of rows per second, each row containing the following columns:
    - timestamp (TimestampType)
    - value (LongType)
  Where timestamp is the time of message dispatch, and value is of Long type containing the message count, starting from 0 as the first row. 
  This source is intended for testing and benchmarking. You can also have a look at "Socket Source"
  
  https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  """
  
  raw_stream_data = (
    spark
      .readStream
      .format("rate") # search for "Rate source" on this page https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
      .option("rowsPerSecond", 500)
      .load()
  )
  
  stream_data = (
    raw_stream_data
      .withColumn("loan_id", 10000 + F.col("value"))
      .withColumn("funded_amnt", (F.rand() * 5000 + 5000).cast("integer"))
      .withColumn("paid_amnt", F.col("funded_amnt") - (F.rand() * 2000))
      .withColumn("addr_state", random_state())
      .withColumn("workload_source", F.lit(workload_source))
  )
    
  if schema_ok:
    stream_data = stream_data.select("loan_id", "funded_amnt", "paid_amnt", "addr_state", "workload_source", "timestamp")
      
  query = (stream_data.writeStream
    .format(table_format)
    .option("checkpointLocation", f"{my_checkpoint_dir}/{workload_source}/")
    .trigger(processingTime = "5 seconds")
    .table(table_name)
  )

  return query

# COMMAND ----------

# Function to stop all streaming queries 
def stop_all_streams():
    print("Stopping all streams")
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("Stopped all streams")
    dbutils.fs.rm(my_checkpoint_dir, True) # delete all checkpoints
    return

def cleanup_paths_and_tables():  
    dbutils.fs.rm(f"{my_checkpoint_dir}", True) # delete all checkpoints
    for table in [f"{db}.loans_parquet", f"{db}.loans_delta", f"{db}.loans_delta2"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    return
    
cleanup_paths_and_tables()

# COMMAND ----------

# Download files to the local filesystem
import os
import wget
import sys
import shutil

 
sys.stdout.fileno = lambda: False # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'   

LOCAL_DIR = f"{os.getcwd()}/{current_user}/deltaDemo"

if os.path.isdir(LOCAL_DIR): shutil.rmtree(LOCAL_DIR)
os.makedirs(LOCAL_DIR)
url = "https://github.com/data-derp/small-exercises/blob/master/real-world-structured-streaming/loan-risks.snappy.parquet?raw=true"

filename = url.split("/")[-1].replace("?raw=true", "").replace("snappy.parquet", "parquet")
saved_filename = wget.download(url, out = f"{LOCAL_DIR}/{filename}")
print(f"Saved in: {saved_filename}")

dbutils.fs.cp(f"file:{saved_filename}", f"{working_directory}/{filename}")

# COMMAND ----------

# MAGIC %md # Getting started with <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC 
# MAGIC An open-source storage layer for data lakes that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Open Format**: Stored as Parquet format in blob storage.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Convert to Delta Lake format

# COMMAND ----------

# MAGIC %md Delta Lake is 100% compatible with Apache Spark&trade;, which makes it easy to get started with if you already use Spark for your big data workflows.
# MAGIC Delta Lake features APIs for **SQL**, **Python**, and **Scala**, so that you can use it in whatever language you feel most comfortable in.

# COMMAND ----------

# MAGIC %md <img src="https://databricks.com/wp-content/uploads/2020/12/simplysaydelta.png" width=600/>

# COMMAND ----------

# MAGIC %md In **Python**: Read your data into a Spark DataFrame, then write it out in Delta Lake format directly, with no upfront schema definition needed.

# COMMAND ----------

parquet_path = f"{working_directory}/{filename}"

df = (spark.read.format("parquet").load(parquet_path)
      .withColumn("workload_source", F.lit("batch"))
      .withColumn("timestamp", F.current_timestamp()))

df.write.format("delta").mode("overwrite").saveAsTable("loans_delta") # save DataFrame as a registered table in your Databricks-managed metastore

delta_df = spark.sql("select * from loans_delta") # read from a registered table within your metastore
delta_df.write.format("parquet").mode("overwrite").save(f"{working_directory}/loans_parquet/") # you can also save your parquets to a specific path (doesn't have to be saved as a registered table)

# COMMAND ----------

# MAGIC %md **SQL Option 1**: Use `CREATE TABLE` statement with SQL (no upfront schema definition needed)

# COMMAND ----------

print(f"Your parquet file is located here: {working_directory}/loans_parquet/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE loans_delta2
# MAGIC USING delta
# MAGIC AS SELECT * FROM parquet.`/FileStore/YOURUSERNAME/deltaDemo/loans_parquet/`

# COMMAND ----------

# MAGIC %md **SQL Option 2**: Use `CONVERT TO DELTA` to convert Parquet files to Delta Lake format **in place**

# COMMAND ----------

print(f"Your parquet file is located here: {working_directory}/loans_parquet/")

# COMMAND ----------

# MAGIC %md Use the above output to create a table from the parquet location.

# COMMAND ----------

# COMMAND ----------

# MAGIC %sql CONVERT TO DELTA parquet.`/FileStore/YOURUSERNAME/deltaDemo/loans_parquet/`

# COMMAND ----------

# MAGIC %md ### View the data in the Delta Lake table
# MAGIC **How many records are there, and what does the data look like?**

# COMMAND ----------

spark.sql("select count(*) from loans_delta").show()
spark.sql("select * from loans_delta").show(3)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified batch + streaming data processing with multiple concurrent readers and writers

# COMMAND ----------

# MAGIC %md ### Write 2 different data streams into our Delta Lake table at the same time.
# MAGIC  ⚠️ BE SURE YOU STOP THE STREAMS BELOW TO AVOID INCURRUING CHARGES ⚠️

# COMMAND ----------

# Set up 2 streaming writes to our table
stream_query_A = generate_and_append_data_stream(table_format="delta", table_name="loans_delta", schema_ok=True, workload_source='stream A')
stream_query_B = generate_and_append_data_stream(table_format="delta", table_name="loans_delta", schema_ok=True, workload_source='stream B')

# COMMAND ----------

# MAGIC %md ### Create 2 continuous streaming readers of our Delta Lake table to illustrate streaming progress.

# COMMAND ----------

# Streaming read #1
display(spark.readStream.format("delta").table("loans_delta").groupBy("workload_source").count().orderBy("workload_source"))

# COMMAND ----------

# Streaming read #2
display(spark.readStream.format("delta").table("loans_delta").groupBy("workload_source", F.window("timestamp", "10 seconds")).count().orderBy("window"))

# COMMAND ----------

# MAGIC %md ### You can even run a batch query on the table in the meantime

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT addr_state, COUNT(*)
# MAGIC FROM loans_delta
# MAGIC GROUP BY addr_state

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

stop_all_streams()

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) ACID Transactions

# COMMAND ----------

# MAGIC %md View the Delta Lake transaction log

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY loans_delta

# COMMAND ----------

# MAGIC %md <img src="https://databricks.com/wp-content/uploads/2020/09/delta-lake-medallion-model-scaled.jpg" width=1012/>

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Enforcement to protect data quality

# COMMAND ----------

# MAGIC %md To show you how schema enforcement works, let's create a new table that has an extra column -- `credit_score` -- that doesn't match our existing Delta Lake table schema.

# COMMAND ----------

# MAGIC %md #### Write DataFrame with extra column, `credit_score`, to Delta Lake table

# COMMAND ----------

# Generate `new_data` with additional column
new_column = [StructField("credit_score", IntegerType(), True)]
new_schema = StructType(spark.table("loans_delta").schema.fields + new_column)
data = [(99997, 10000, 1338.55, "CA", "batch", datetime.now(), 649),
        (99998, 20000, 1442.55, "NY", "batch", datetime.now(), 702)]

new_data = spark.createDataFrame(data, new_schema)
new_data.printSchema()

# COMMAND ----------

# Uncommenting this cell will lead to an error because the schemas don't match.
# Attempt to write data with new column to Delta Lake table
new_data.write.format("delta").mode("append").saveAsTable("loans_delta")

# COMMAND ----------

# MAGIC %md **Schema enforcement helps keep our tables clean and tidy so that we can trust the data we have stored in Delta Lake.** The writes above were blocked because the schema of the new data did not match the schema of table (see the exception details). See more information about how it works [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html).

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Evolution to add new columns to schema
# MAGIC 
# MAGIC If we *want* to update our Delta Lake table to match this data source's schema, we can do so using schema evolution. Simply add the following to the Spark write command: `.option("mergeSchema", "true")`

# COMMAND ----------

new_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("loans_delta")

# COMMAND ----------

# MAGIC %sql SELECT * FROM loans_delta WHERE loan_id IN (99997, 99998)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Time Travel

# COMMAND ----------

# MAGIC %md Delta Lake’s time travel capabilities simplify building data pipelines for use cases including:
# MAGIC 
# MAGIC * Auditing Data Changes
# MAGIC * Reproducing experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC <img src="https://github.com/risan4841/img/blob/master/transactionallogs.png?raw=true" width=250/>
# MAGIC 
# MAGIC You can query snapshots of your tables by:
# MAGIC 1. **Version number**, or
# MAGIC 2. **Timestamp.**
# MAGIC 
# MAGIC using Python, Scala, and/or SQL syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to the [docs](https://docs.delta.io/latest/delta-utility.html#history), or [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md #### Review Delta Lake Table History for  Auditing & Governance
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY loans_delta

# COMMAND ----------

# MAGIC %md #### Use time travel to select and view the original version of our table (Version 0).
# MAGIC As you can see, this version contains the original 14,705 records in it.

# COMMAND ----------

spark.sql("SELECT * FROM loans_delta VERSION AS OF 0").show(3)
spark.sql("SELECT COUNT(*) FROM loans_delta VERSION AS OF 0").show()

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM loans_delta

# COMMAND ----------

# MAGIC %md #### Rollback a table to a specific version using `RESTORE`

# COMMAND ----------

# MAGIC %sql RESTORE loans_delta VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM loans_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support: `DELETE`, `UPDATE`, `MERGE INTO`
# MAGIC 
# MAGIC Delta Lake brings ACID transactions and full DML support to data lakes.
# MAGIC 
# MAGIC >Parquet does **not** support these commands - they are unique to Delta Lake.

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) `DELETE`: Handle GDPR or CCPA Requests on your Data Lake

# COMMAND ----------

# MAGIC %md Imagine that we are responding to a GDPR data deletion request. The user with loan ID #4420 wants us to delete their data. Here's how easy it is.

# COMMAND ----------

# MAGIC %md **View the user's data**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loans_delta WHERE loan_id=4420

# COMMAND ----------

# MAGIC %md **Delete the individual user's data with a single `DELETE` command using Delta Lake.**
# MAGIC 
# MAGIC Note: The `DELETE` command isn't supported in Parquet.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM loans_delta WHERE loan_id=4420;
# MAGIC -- Confirm the user's data was deleted
# MAGIC SELECT * FROM loans_delta WHERE loan_id=4420

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use time travel and `INSERT INTO` to add the user back into our table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO loans_delta
# MAGIC SELECT * FROM loans_delta VERSION AS OF 0
# MAGIC WHERE loan_id=4420

# COMMAND ----------

# MAGIC %sql SELECT * FROM loans_delta WHERE loan_id=4420

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) `UPDATE`: Modify the existing records in a table in one command

# COMMAND ----------

# MAGIC %sql UPDATE loans_delta SET funded_amnt = 33000 WHERE loan_id = 4420

# COMMAND ----------

# MAGIC %sql SELECT * FROM loans_delta WHERE loan_id = 4420

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Support Change Data Capture Workflows & Other Ingest Use Cases via `MERGE INTO`

# COMMAND ----------

# MAGIC %md
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif" alt='Merge process' width=600/>
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# Create merge table with 1 row update, 1 insertion
data = [(4420, 22000, 21500.00, "NY", "update", datetime.now()),  # record to update
        (99999, 10000, 1338.55, "CA", "insert", datetime.now())]  # record to insert
schema = spark.table("loans_delta").schema
spark.createDataFrame(data, schema).createOrReplaceTempView("merge_table")
spark.sql("SELECT * FROM merge_table").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO loans_delta AS l
# MAGIC USING merge_table AS m
# MAGIC ON l.loan_id = m.loan_id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql SELECT * FROM loans_delta WHERE loan_id IN (4420, 99999)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) File compaction and performance optimizations = faster queries

# COMMAND ----------

# MAGIC %md ### Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vacuum deletes all files no longer needed by the current version of the table.
# MAGIC VACUUM loans_delta

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Cache table in memory (Databricks Delta Lake only)

# COMMAND ----------

# MAGIC %sql CACHE SELECT * FROM loans_delta

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Z-Order Optimize (Databricks Delta Lake only)

# COMMAND ----------

# MAGIC %sql OPTIMIZE loans_delta ZORDER BY addr_state

# COMMAND ----------

cleanup_paths_and_tables()

# COMMAND ----------

# MAGIC %md <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>

# COMMAND ----------

# MAGIC %md #Join the community!
# MAGIC 
# MAGIC 
# MAGIC * [Delta Lake on GitHub](https://github.com/delta-io/delta)
# MAGIC * [Delta Lake Slack Channel](https://delta-users.slack.com/) ([Registration Link](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA))
# MAGIC * [Public Mailing List](https://groups.google.com/forum/#!forum/delta-users)
