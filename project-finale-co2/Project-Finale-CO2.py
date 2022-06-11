# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Wrapping Up with Your CO2 Project
# MAGIC 
# MAGIC - Well done for completing the CO2 project at a very impressive speed!
# MAGIC - Let's now reap the fruits of your labour by looking at the final data
# MAGIC - Since several participants seemed very interested in Spark optimization, we'll also quickly go through that :)
# MAGIC 
# MAGIC #### Practical Performance Optimization Tips

# COMMAND ----------

# MAGIC %pip install wget

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the `YOURBUCKETHERE` with your bucket name

# COMMAND ----------

country_df = spark.read.format("parquet").load("s3://YOURBUCKETHERE/data-transformation/CountryEmissionsVsTemperatures.parquet/")

# COMMAND ----------

global_df = spark.read.format("parquet").load("s3://YOURBUCKETHERE/data-transformation/GlobalEmissionsVsTemperatures.parquet/")

# COMMAND ----------

import pandas as pd

# Download files to the local filesystem
import os
import wget
import sys
 
sys.stdout.fileno = lambda: False # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'   

DATA_URL = "https://raw.githubusercontent.com/data-derp/small-exercises/master/project-finale-co2/country-and-continent-codes-list-csv.csv"

FILENAME = DATA_URL.split("/")[-1]
wget.download(DATA_URL, FILENAME)
  
# Set up a DBFS directory to keep a copy of our data in a distributed filesystem (rather than local/single-node)
# Copy over the files from the local filesystem to our new DBFS directory (mini-project)
# (so that Spark can read in a performant manner from dbfs:/)

EXERCISE_DIR = "dbfs:/FileStore/mini-project/project-finale/"
LOCAL_DIR = os.getcwd()

dbutils.fs.rm(EXERCISE_DIR, True) # delete directory, start fresh
dbutils.fs.mkdirs(EXERCISE_DIR)

# for filename in filenames: # copy from local file system into a distributed file system such as DBFS
dbutils.fs.cp(f"file:{LOCAL_DIR}/{FILENAME}", f"""{EXERCISE_DIR}/{FILENAME}""")
  
dbutils.fs.ls(EXERCISE_DIR)

# Copy locally because Python can't read from dbfs in the community edition
dbutils.fs.cp("dbfs:/FileStore/mini-project/project-finale/country-and-continent-codes-list-csv.csv", "file:///tmp/project-finale/country-and-continent-codes-list-csv.csv")


from IPython.display import Image

continents_pandas = pd.read_csv("/tmp/project-finale/country-and-continent-codes-list-csv.csv")
continents_pandas = continents_pandas.rename(columns={"Continent_Name": "Continent", "Country_Name": "Country"})
continents_pandas = continents_pandas[["Country", "Continent"]]
continents_pandas["Country"] = continents_pandas["Country"].str.split(",").str[0].str.split("(").str[0].str.strip()

# COMMAND ----------

dbutils.fs.rm("continents.parquet/", True)

continents_df = spark.createDataFrame(continents_pandas)
continents_df.write.format("parquet").save("dbfs:/continents.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### To understand how to improve performance, we need to first understand bottlenecks
# MAGIC 
# MAGIC - Where does shuffling in Spark jobs occur? **Answer: between Stages **

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1") # turn off autoBroadCastJoin to demonstrate shuffling!

# COMMAND ----------

continents_df = spark.read.format("parquet").load("dbfs:/continents.parquet")
joined_df = country_df.join(continents_df, on="Country", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To see the Spark DAG of the job, you need to apply an **action**
# MAGIC 
# MAGIC Examples of actions:
# MAGIC - count
# MAGIC - toPandas

# COMMAND ----------

joined_pandas = joined_df.toPandas()

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # turn on autoBroadCastJoin to demonstrate the power of broadcasting

# COMMAND ----------

continents_df = spark.read.format("parquet").load("dbfs:/continents.parquet")
joined_df = country_df.join(continents_df, on="Country", how="left")

# COMMAND ----------

joined_pandas = joined_df.toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Tip 1: Broadcasting
# MAGIC 
# MAGIC - So what does broadcasting do? It sends a full copy of the DataFrame to every node so that less data needs to be shuffled through the network
# MAGIC - Spark will **automatically broadcast** small DataFrames (less than 10MB), however you can explicitly force a DataFrame to be broadcast joined:
# MAGIC     - `import pyspark.sql.functions as F` then `joined_df = country_df.join(F.broadcast(continents_df), on="Country", how="left")`
# MAGIC - **Limitations: only works for small datasets**
# MAGIC 
# MAGIC **What if I want to broadcast something other than a DataFrame?**
# MAGIC - You can use `sc.broadcast` to broadcast arbitrary variables to every node in the cluster
# MAGIC - Broadcast variables are **read-only**
# MAGIC - See this article for an example: https://databricks.com/blog/2019/12/05/processing-geospatial-data-at-scale-with-databricks.html
# MAGIC   - look for `sc.broadcast`
# MAGIC 
# MAGIC ### Tip 2: Caching/Persisting
# MAGIC - Remember that, by default, Spark DAGs are always recomputed from the start with each and every **action**
# MAGIC - If you want to avoid recomputing things, you can consider using `.cache()` and/or `.persist()`
# MAGIC - **However:** it's much more practical to write intermediate tables to S3 (so that they are persisted **forever**, in case you want to come back to it another day) 
# MAGIC 
# MAGIC ### Tip 3: Partitioning!!!
# MAGIC **But most importantly, don't forget about .repartition() and .write.partitionBy()**
# MAGIC - nothing beats this
# MAGIC - use `.repartition(col1, col2, col3)` if you want to ensure that there's 1 .snappy.parquet file per each partition
