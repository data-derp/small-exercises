# Databricks notebook source
# MAGIC %md
# MAGIC 💡 _Run the following command to initalise data setup and imports_

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

import pandas as pd
import os

os.listdir(f"/dbfs{working_directory}")

# COMMAND ----------

dbutils.fs.ls("./databricks-datasets/flights")

# COMMAND ----------

# DBTITLE 1,SparkSession
# MAGIC %md
# MAGIC 1. Creating Spark DataFrame using list
# MAGIC 2. Creating Spark DataFrame using set
# MAGIC 3. Creating Spark DataFrame using Pandas DataFrame
# MAGIC 4. Create a DataFrame for a range of numbers
# MAGIC 6. Register User Defined Functions (UDFs).

# COMMAND ----------

# MAGIC %md
# MAGIC SparkSession is the driver program (encapsulates SparkContext) that interacts with the underlying cluster.

# COMMAND ----------

#spark session is available through
spark

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Creating Spark DataFrame using list

# COMMAND ----------

"""
using a list to create a spark DataFrame
"""
int_list = [[i] for i in range(100)]
df = spark.createDataFrame(int_list, ['Numbers'])
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Creating Spark DataFrame using set

# COMMAND ----------

"""
Create a dataFrame using a set
"""
a_set = (('Manu','100'),('Aditya','90'))
df = spark.createDataFrame(a_set, ['Name', 'EnergyMeter'])
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Creating Spark DataFrame using Pandas DataFrame

# COMMAND ----------

"""
Create a dataFrame using pandas dataFrame
"""
pandas_dataframe = pd.DataFrame({'first':range(200), 'second':range(300,500)})
df = spark.createDataFrame(pandas_dataframe)
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Creating Spark DataFrame from a range of numbers
# MAGIC We'll be looking at [range](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.SparkSession.range.html) method to create a Spark DataFrame.

# COMMAND ----------

spark.range(100).collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### DataFrameReaders
# MAGIC DataFrameReader class in pyspark let's one read data into a spark DataFrame

# COMMAND ----------

'''
Spark DataFrameReader API is accessed using the following 
variable
'''
spark.read

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

"""
reading a datafile using dataframe reader, more on this later
"""
file_path = '/databricks-datasets/bikeSharing/data-001/day.csv'
df = spark.read\
  .option('inferSchema', True)\
  .csv(file_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Register User Defined Functions (UDFs)
# MAGIC A **User defined function** or UDF is a function defined by the user to do a ***custom tranformation** on the dataset leveraging the spark framework. To apply the method on the dataset taking advantage of the spark framework one must 'register' the UDF. More on this later.

# COMMAND ----------

data_path = "/databricks-datasets/asa/airlines/1987.csv"
df = spark.read\
  .option('inferSchema', True)\
  .option('header', True)\
  .csv(data_path)
display(df)

# COMMAND ----------

#Spark has it's own datatypes
from pyspark.sql.types import IntegerType
# define a function that adds 1 to a record
def addOne(rec):
  return rec + 1
# register function
udf_fun = spark.udf.register('add1', addOne, IntegerType())
udf_fun

# COMMAND ----------

import pyspark.sql.functions as F

temp_df = df.select(udf_fun("DayOfWeek").alias('DOW+1'),F.col("DayOfWeek").alias('DOW'))
display(temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _pyspark.sql.functions module contains many different utility and transformation functions_

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC **Scenario 4.1**
# MAGIC <br>
# MAGIC **Dataset**: /databricks-datasets/bikeSharing/data-001/day.csv
# MAGIC <br>
# MAGIC **Problem statement**: Read the dataFrame **with headers**. Refer [DataFrameReader API](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrameReader).

# COMMAND ----------

'''
write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

#SOLUTION
file_path = "/databricks-datasets/bikeSharing/data-001/day.csv"
df = spark.read\
  .option('inferSchema', True)\
  .option('header', True)\
  .csv(file_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario 4.2**
# MAGIC <br>
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
# MAGIC 2. Define a function that catogarizes flights into the following bucket based on "ArrDelay".
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

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [DataFrame API](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame)
# MAGIC 2. [DataFrameReader](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrameReader)
# MAGIC 3. [Col function](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.functions.col)
# MAGIC 3. [User defined functions](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.functions.udf)

# COMMAND ----------

