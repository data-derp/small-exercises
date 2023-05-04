# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## A Gentle Introduction to Spark on Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Welcome! Databricks is a cloud-first environment where Data Analysts, Data Scientists, and Data Engineers can collaborate and run **fully-managed Apache Spark** on the cloud. <br>
# MAGIC It's basically very similar to a Jupyter Notebook but much better for Spark and working with Big Data.
# MAGIC 
# MAGIC **Additional Features**: <br>
# MAGIC - Revision History and Version Control integration with: GitHub, Bitbucket, Azure DevOps
# MAGIC - Lots of different clusters/compute instances you can create then easily turn on/off
# MAGIC - Job scheduling for no additional cost

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Databricks File System (DBFS)
# MAGIC - Databricks comes with a convenient 'file system' called DBFS. Actually, this is just an abstraction of some object storage service (e.g. Amazon S3 or Azure Blob Storage)
# MAGIC - Therefore, for large production jobs, I would actually recommend reading/writing to **S3 or Blob Storage explicitly**, in order to have universal access and control over your data
# MAGIC - However, for small/quick tasks DBFS can be pretty convenient :) 
# MAGIC 
# MAGIC ### Databricks Utilities (DBUtils)
# MAGIC - You can access DBFS via `dbutils` (comes pre-installed with all Databricks clusters)
# MAGIC - dbutils actually has many other useful features such as: **programmatically executing other notebooks** & **accessing/managing secrets**
# MAGIC - For more info: https://docs.databricks.com/dev-tools/databricks-utils.html

# COMMAND ----------

dbutils.fs.ls(".") # use the FS module of dbutils to access DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Your entry point to Spark:
# MAGIC - SparkSession (often named as the variable `spark`)
# MAGIC 
# MAGIC All Databricks runtimes automatically names the SparkSession this way already.

# COMMAND ----------

spark

# COMMAND ----------

dbutils.fs.ls("./databricks-datasets/amazon/data20K/")

# COMMAND ----------

df = spark.read.format("parquet").load("dbfs:/databricks-datasets/amazon/data20K")

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Don't be afraid of making lots of DataFrame variables!
# MAGIC 
# MAGIC - after all, they're just a bunch of lazy transformations (in DAGs)
# MAGIC - there's no real resource consumption! in fact, it can even be better for clarity/debugging

# COMMAND ----------

import pyspark.sql.functions as F

# let's add a new column: instead of 1, 2, 3, 4, 5 let's make it a percentage between 0 and 1
# e.g. 4/5 => 80%
brand_new_column = F.col("rating_percentage") / 2 # with Column expressions, you don't need a column to even exist yet!

df = df.withColumn("rating_percentage", (F.col("rating") / F.lit(5)) * F.lit(100))
display(df.withColumn("half_rating_percentage", brand_new_column))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Experiment with Casting Columns

# COMMAND ----------

df.schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

df_cast_test = df

df_cast_test = df_cast_test.withColumn("rating", F.col("rating").cast(IntegerType()))
df_cast_test = df_cast_test.withColumn("rating_percentage", (F.col("rating") / F.lit(5)) * F.lit(100))
df_cast_test = df_cast_test.withColumn("review_timestamp", F.col("review").cast(TimestampType())) # let's see what happens

display(df_cast_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Is there another way to create or rename columns?
# MAGIC 
# MAGIC Yes! Using the `alias` method

# COMMAND ----------

df_select_test = df

any_arbitrary_expression = (F.col("rating") + F.lit(1)).alias("arbitrary_expr")

# to add a new column, put "*" followed by your next arguments
df_select_test = df_select_test.select("*", any_arbitrary_expression) # you can also rename columns with alias()
display(df_select_test)

# COMMAND ----------

display(df_select_test.select(["rating_percentage", "review"]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Can I make my OWN DataFrames??

# COMMAND ----------

import pandas as pd
from datetime import datetime 

pandas_df = pd.DataFrame({"x": [1, 2, 3, 4], "y": ["A", "B", "C", "D"]})
pandas_df["t"] = [datetime.now()] * len(pandas_df)
pandas_df

# COMMAND ----------

spark_df = spark.createDataFrame(pandas_df)
spark_df = spark_df.withColumn("z", F.col("t").cast(LongType())) # convert to Epoch seconds
spark_df = spark_df.withColumn("t_recreated", F.col("z").cast(TimestampType()))

# COMMAND ----------

spark_df.collect()

# COMMAND ----------

display(spark_df)
