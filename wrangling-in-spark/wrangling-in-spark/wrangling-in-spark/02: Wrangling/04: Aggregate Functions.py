# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregation Functions
# MAGIC 
# MAGIC In an aggregation, we specify a key or grouping and an aggregation function that specifies how we should transform one or more columns. This function must produce one result for each grouping, given multiple input values.
# MAGIC 
# MAGIC The aggregation functions that we'll cover in this notebook are the following:
# MAGIC 1. count
# MAGIC 2. min
# MAGIC 3. max
# MAGIC 4. sum
# MAGIC 5. avg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv

# COMMAND ----------

fl = "/databricks-datasets/power-plant/data/Sheet1.tsv"
df = spark.read\
  .format('csv')\
  .option('inferSchema', True)\
  .option('header', True)\
  .option('delimiter', '\t')\
  .load(fl)

# COMMAND ----------

from pyspark.sql import functions as f


# COMMAND ----------

# MAGIC %md
# MAGIC ## Count
# MAGIC This function counts the number of records in a DataFrame

# COMMAND ----------

df.filter(f.col("AT")>14).count()
df.filter(f.col("V")<14).count()
df.filter(f.col("RH")<=14).count()

# COMMAND ----------

# MAGIC %md
# MAGIC There are other ways to count:
# MAGIC * [countDistinct](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.countDistinct.html): returns the number of distinct elements in a group
# MAGIC * [approx_count_distinct](https://docs.databricks.com/sql/language-manual/functions/approx_count_distinct.html): returns the estimated number of distinct values in expr within the group

# COMMAND ----------

df.select(f.count("PE")).show()
df.select(f.countDistinct("AT")).show()
df.select(f.approx_count_distinct("AT", 0.1)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sum

# COMMAND ----------

df.select(f.sum("AT")).show()
df.select(f.sum_distinct("V")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Min and Max

# COMMAND ----------

df.select(f.min("AT"), f.max("AT")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avg

# COMMAND ----------

df.select(
  f.count("V"),
  f.min("V"),
  f.max("V"),
  f.mean("V").alias("mean()"),
  f.avg("V").alias("avg()"),
  f.expr("avg(V)").alias("expr(avg)"),
  f.expr("mean(V)").alias("expr(mean)")
  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise
# MAGIC 
# MAGIC Aggregate Function Exercises
# MAGIC 1. Try sum_distinct() a data frame.
# MAGIC 2. Try rounding off the result of sum() to 3 decimal places.
# MAGIC 3. Try getting the max() out of the totals( sum() ) of all the columns of the data frame.
# MAGIC 4. Try applying max() & min() on the count of each column available in data frame.
# MAGIC 
# MAGIC **Note:** this exercise is deliberately open-ended without a solution

# COMMAND ----------

'''
Write code here
'''