# Databricks notebook source
# DBTITLE 1,Date time functions
# MAGIC %md
# MAGIC 1. Parsing dates
# MAGIC 2. Parsing date type columns
# MAGIC 3. Formatting timestamp into strings
# MAGIC 4. Date aritmetic (+,- ,<,>)

# COMMAND ----------

# Turn off AQE
# AQE was introduced in Spark 3.0 which optimises your queries
# We turn it off here to demonstrate natural behaviour
# More info: https://docs.databricks.com/spark/latest/spark-sql/aqe.html#enable-and-disable-adaptive-query-execution

print(spark.conf.get('spark.databricks.optimizer.adaptive.enabled'))
spark.conf.set('spark.databricks.optimizer.adaptive.enabled','false')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Parsing dates 

# COMMAND ----------

from pyspark.sql import functions as f
fp = "/databricks-datasets/bikeSharing/data-001/day.csv"
df = spark.read\
  .format("csv")\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(fp)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Spark follows Java like date and timestamp standards_

# COMMAND ----------

##  coercing from string to date type
df.select(f.to_date(f.col("dteday")))
## specifying date format 
df.select(f.to_date(f.col("dteday"),"dd-MM-yyyy")).show(5)
## getting timestamps of a date type
df.select(f.to_timestamp(f.col("dteday")).alias("Timestamp")).show(5)
## coercing timestamp back to string
df.select( f.to_date(f.to_timestamp(f.col("dteday")),"yyyy-MM-dd").alias("Date String")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Date aritmetic (+,- ,<,>)

# COMMAND ----------

"Lets see what is the max wind speed after year 2011"
df.filter( f.to_date( f.col("dteday") ) > f.to_date(f.lit("2011")) ).select(f.max("windspeed")).show()


# COMMAND ----------

"What is the humidity between given pair of dates or years"
df.select( f.col("hum") ).where( (f.col("dteday") < f.to_date(f.lit("2012-05-05"))) &  (f.col("dteday") >= f.to_date(f.lit("2011-05-05")))).collect()
df.select( f.col("hum") ).where( (f.col("dteday") < f.to_date(f.lit("2012"))) &  (f.col("dteday") >= f.to_date(f.lit("2011")))).collect() ## what is the max hum of that period ? 

# COMMAND ----------

# DBTITLE 1,String functions
# MAGIC %md
# MAGIC 1. Regular expressions
# MAGIC 2. instr
# MAGIC 3. locate

# COMMAND ----------

#reading the dataset
covid_fp = "/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv"
df2  = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema", True)\
        .load(covid_fp)
# df2.columns
display(df2)
df2.printSchema()

# COMMAND ----------

## Regular exp - regexp_replace
df2.select(f.regexp_replace(f.col("infection_case"), "patient", "PATIENT"),f.col("infection_case")).show(10)

# COMMAND ----------

## Regular exp - regexp_extract
df2.select(f.regexp_extract(f.col("infection_case"), "patient" ,0).alias("extracted"),f.col("infection_case")).show(10)

# COMMAND ----------

## Regular exp - instr
contains_church = f.instr(f.col("infection_case"), "Church") >= 1
contains_hospital = f.instr(f.col("infection_case"), "Hospital") >= 1
df2.withColumn("infection_case_church_hospital", contains_church | contains_hospital)\
.select("infection_case", "infection_case_church_hospital").collect()

# COMMAND ----------

# DBTITLE 1,Math functions
# MAGIC %md
# MAGIC 1. Basic arithmatic (sum, sqrt, exp)
# MAGIC 2. statistical function (stddev, variance, mean)

# COMMAND ----------

# Math fun(s) - sum : Aggregate function: returns the sum of all values in the expression.
df.select(
  f.sum(f.col("temp")).alias("Sum(temp)")
).collect()
# Math fun(s) - sqrt : Computes the square root of the specified float value.
df.select(
  f.col("windspeed"),
  f.sqrt("windspeed").alias("Sqrt(windspeed)")
).show(10)
# Math fun(s) - exp : Computes the exponential of the given value.
df.select(
  f.col("temp"),
  f.exp("temp").alias("Exp(temp)")
).show(10)

# COMMAND ----------

# Stastical fun(s) :
# stddev : Aggregate function: returns the unbiased sample standard deviation of the expression in a group.
# variance : Aggregate function: returns the unbiased sample variance of the values in a group.
# mean : Aggregate function: returns the average of the values in a group.

df.select(
  f.stddev(f.col("hum")).alias("stddev(hum)"),
  f.variance(f.col("hum")).alias("variance(hum)"),
  f.mean(f.col("hum")).alias("mean(hum)"),
).show()


# COMMAND ----------

# DBTITLE 1,UDF functions
# MAGIC %md
# MAGIC 
# MAGIC 1. Registering a UDF
# MAGIC 2. Using UDF on a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC User defined function or UDF provide extensiblity to the already rich spark native library by providing the user with the ability of writing custom transformation functions.

# COMMAND ----------

display(df2)

# COMMAND ----------

# Register UDF
from datetime import datetime, time
from pyspark.sql.types import StringType

# UDF1 registration method 1
to_upper = udf(lambda s: s.upper(), StringType())

# UDF2 registration method 2 (using decorator)
@udf
def t_year(t):
  if t is not None:
    return t.split("-")[0]

# Using registred UDF
df2.select(
  to_upper(f.col("province")),
  t_year(f.col("symptom_onset_date"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario 11.1**
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