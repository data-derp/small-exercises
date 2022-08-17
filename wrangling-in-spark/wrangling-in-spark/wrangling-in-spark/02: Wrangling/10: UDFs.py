# Databricks notebook source
# MAGIC %md
# MAGIC ## UDFs
# MAGIC A **User defined function** or UDF is a function defined by the user to do a **custom tranformation** on a dataset, while still leveraging the Spark framework. We must first register the function with Spark before using it.
# MAGIC 
# MAGIC * [Read more about UDFs](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.functions.udf)
# MAGIC * [Some UDF examples](https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

data_path = "/databricks-datasets/asa/airlines/1987.csv"
df = spark.read\
  .option('inferSchema', True)\
  .option('header', True)\
  .csv(data_path)
display(df)

# COMMAND ----------

from pyspark.sql.types import IntegerType

def addOne(rec):
    return rec + 1

udf_fun = spark.udf.register('add1', addOne, IntegerType())
udf_fun

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF Registration
# MAGIC Note: There are multiple ways of registering a UDF.

# COMMAND ----------

covid_file_path = "/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv"
covid_df  = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema", True)\
        .load(covid_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1:

# COMMAND ----------

from pyspark.sql.types import StringType
to_upper = udf(lambda s: s.upper(), StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2:

# COMMAND ----------

@udf
def t_year(t):
    if t is not None:
        return t.split("-")[0]


# COMMAND ----------

# MAGIC %md
# MAGIC You can use both!

# COMMAND ----------

from pyspark.sql import functions as f

covid_df.select(
  to_upper(f.col("province")),
  t_year(f.col("symptom_onset_date"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataset**: /databricks-datasets/asa/airlines/1987.csv 
# MAGIC <br>
# MAGIC **Problem statement**: Create a UDF to create a column "Arrival Delay Categories" which buckets a record based on "ArrDelay" coulmn using following conditions
# MAGIC <br>if _ArrDelay < 0_ -> 0
# MAGIC <br>if _ArrDelay = 0_ -> 1
# MAGIC <br>if _0 < ArrDelay_< 30  -> 2
# MAGIC <br>if ArrDelay > 30  -> 3
# MAGIC <br>
# MAGIC **Steps to follow**:
# MAGIC 1. Read the dataset
# MAGIC 2. Define a function that categorizes flights into the following bucket based on "ArrDelay".
# MAGIC 3. Register UDF
# MAGIC 4. Apply UDF

# COMMAND ----------

'''
write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

data_path = "/databricks-datasets/asa/airlines/1987.csv"
#read dataset
df = spark.read\
  .option('inferSchema', True)\
  .option('header', True)\
  .csv(data_path)
display(df)

# COMMAND ----------

#import udf and col functions
from pyspark.sql.functions import udf, col
#import Spark dataType
from pyspark.sql.types import StringType

#create function that takes a record and output's corresponding bucket value
def bucketDelay(rec):
  try:
    rec = int(rec)
  except Exception as e:
    return rec
  output = None
  if rec < 0:
    output = 0
  elif rec == 0:
    output = 1
  elif rec<=30:
    output = 2
  else:
    output = 3
  return output

#register the UDF
bucketDelay = spark.udf.register('bucketDelay', bucketDelay, StringType())
#Apply the UDF on columns
temp_df = df.select(col('ArrDelay'),bucketDelay(col('ArrDelay')).alias('Arrival Delay Categories'))
#Display result
display(temp_df)