# Databricks notebook source
# MAGIC %md
# MAGIC # Date/Time Functions
# MAGIC 1. Parsing dates
# MAGIC 2. Parsing date type columns
# MAGIC 3. Formatting timestamp into strings
# MAGIC 4. Date aritmetic (+,- ,<,>)
# MAGIC 
# MAGIC [Date Time Patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing dates 

# COMMAND ----------

from pyspark.sql import functions as f
file_path = "/databricks-datasets/bikeSharing/data-001/day.csv"
df = spark.read\
  .format("csv")\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(file_path)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Spark follows Java like date and timestamp standards_

# COMMAND ----------

# coerce from string to date type
df.select(f.to_date(f.col("dteday")))

# COMMAND ----------

# specif a date format 
df.select(f.to_date(f.col("dteday"),"dd-MM-yyyy")).show(5)


# COMMAND ----------

# convert to timestampe from date type
df.select(f.to_timestamp(f.col("dteday")).alias("Timestamp")).show(5)


# COMMAND ----------

# coerce timestamp back to string
df.select(f.to_date(f.to_timestamp(f.col("dteday")),"yyyy-MM-dd").alias("Date String")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Aritmetic (+,- ,<,>)

# COMMAND ----------

# MAGIC %md
# MAGIC What is the max wind speed after the year 2011?

# COMMAND ----------

df.filter(f.to_date(f.col("dteday")) > f.to_date(f.lit("2011"))) \
    .select(f.max("windspeed")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC Return all humidity between 05.05.2011 and 05.05.2012

# COMMAND ----------

humidity_df = df.select(f.col("hum"))\
    .where( \
           (f.col("dteday") < f.to_date(f.lit("2012-05-05"))) \
           & (f.col("dteday") >= f.to_date(f.lit("2011-05-05")))) \
    .show()



# COMMAND ----------

# MAGIC %md
# MAGIC What is the max humidity between 2011 and 2012?

# COMMAND ----------

df.select(f.col("hum")).where((f.col("dteday") < f.to_date(f.lit("2012"))) &  (f.col("dteday") >= f.to_date(f.lit("2011")))) \
    .select(f.max("hum")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Math Functions
# MAGIC 1. Basic arithmatic (sum, sqrt, exp)
# MAGIC 2. statistical function (stddev, variance, mean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sum
# MAGIC This is an aggregate function that returns the sum of all values in the expression.

# COMMAND ----------

df.select(
  f.sum(f.col("temp")).alias("Sum(temp)")
).collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sqrt
# MAGIC Computes the square root of the specified float value.

# COMMAND ----------

df.select(
  f.col("windspeed"),
  f.sqrt("windspeed").alias("Sqrt(windspeed)")
).show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ### exp
# MAGIC Computes the exponential of the given value

# COMMAND ----------

df.select(
  f.col("temp"),
  f.exp("temp").alias("Exp(temp)")
).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statistical Functions
# MAGIC 
# MAGIC The follow are aggregate functions
# MAGIC * **stddev**: returns the unbiased sample standard deviation of the expression in a group.
# MAGIC * **variance**: returns the unbiased sample variance of the values in a group.
# MAGIC * **mean**: returns the average of the values in a group.

# COMMAND ----------

df.select(
  f.stddev(f.col("hum")).alias("stddev(hum)"),
  f.variance(f.col("hum")).alias("variance(hum)"),
  f.mean(f.col("hum")).alias("mean(hum)"),
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 
# MAGIC <br>**Dataset**: /databricks-datasets/learning-spark-v2/sf-fire
# MAGIC <br>**Problem Statement**: Arrange the Incident Numbers based on the least time taken by the fire team to close the incident and get formatted time diff in minutes.
# MAGIC <br>**Steps**
# MAGIC 1. Read the dataset in dataFrames as df_fire_incidents.
# MAGIC 2. From df_fire_incidents order all records by least time difference between Arrival time and Close time columns
# MAGIC 3. Order Incident numbers by time difference in descending order

# COMMAND ----------

'''
Write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

import pyspark.sql.functions as f
fire_incidents = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-incidents.csv"
#Reading the dataset
df_fire_incidents = spark.read\
  .format("csv")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load(fire_incidents)\
  .filter(f.col('Incident Number').isNotNull())\
  .dropDuplicates(subset = ['Incident Number'])

# COMMAND ----------

format_ = "MM/dd/yyyy hh:mm:ss a"

df_with_time_diff = df_fire_incidents.withColumn(
  "TimeTakenInMinutes",
  f.to_timestamp(f.col("Close DtTm"), format_).cast("long") - f.to_timestamp(f.col("Arrival DtTm"),format_).cast("long"))

sorted_df = df_with_time_diff.select(f.col("Incident Number"), f.col("Arrival DtTm"), f.col("Close DtTm"), f.col("TimeTakenInMinutes")/60).orderBy(f.desc("TimeTakenInMinutes"))

display(sorted_df)

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [Parsing date time](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)