# Databricks notebook source
# DBTITLE 1,Aggregate functions 
# MAGIC %md
# MAGIC In an aggregation, you will specify a key or grouping and an aggregation function that specifies
# MAGIC how you should transform one or more columns. This function must produce one result for each
# MAGIC group, given multiple input values.
# MAGIC 1. Count
# MAGIC 2. min and max
# MAGIC 3. sum and avg
# MAGIC 4. Grouping (with expression)

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv

# COMMAND ----------

#read the dataset
fl = "/databricks-datasets/power-plant/data/Sheet1.tsv"
df = spark.read\
  .format('csv')\
  .option('inferSchema', True)\
  .option('header', True)\
  .option('delimiter', '\t')\
  .load(fl)

# COMMAND ----------

# inspect, perform operations
from pyspark.sql import functions as f
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Count

# COMMAND ----------

#count operations
#count more than/less than/equal on columns 
df.filter(f.col("AT")>14).count()
df.filter(f.col("V")<14).count()
df.filter(f.col("RH")<=14).count()
# count on data frame 
df.count()
df.select(f.count("PE")).show()
df.select(f.countDistinct("AT")).show()
df.select(f.approx_count_distinct("AT", 0.1)).show() #lesser exeution time, maximum estimation error specified, for big data sets

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sum

# COMMAND ----------

#sum operations
df.select(f.sum("AT")).show()
df.select(f.sum_distinct("V")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Min and Max

# COMMAND ----------

# min and max operations
df.select(f.min("AT"), f.max("AT")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Avg

# COMMAND ----------

# average operation on the column V
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

# DBTITLE 1,Non aggregate functions
# MAGIC %md
# MAGIC 1. Creating an array
# MAGIC 2. na.drop, na.fill and na.replace

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Creating an array

# COMMAND ----------

# create array values
df.take(1) # array of first row
v2 = df.take(5) # a 5Xncol array
print("1: ", v2)
print("2: ", len(v2))
print("2: ", type(v2))
print("3: ", v2[0])
print("4: ", type((v2[0])))
print("5: ", dir(v2[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### na.drop, na.fill and na.replace

# COMMAND ----------

# na.drop
df.na.drop().count() # any
df.na.drop('all').count()
df.na.drop('any').count() #default
df.na.drop('any', 2) # atleast 2 values must be null
df.na.drop('any', 2, subset=['AT','V']) # consider specific list of cols

# COMMAND ----------

# na.fill
df.na.fill('use this string to fill up all NULLs throughout').count()
df.na.fill('use this string to fill up NULLS from specific cols', subset=['AT','V']).show() # consider specific list of cols

# COMMAND ----------

# na.replace
df.na.replace(14.96,49.96).show()

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/COVID/coronavirusdataset"))

# COMMAND ----------

# DBTITLE 1,Collection functions
# MAGIC %md
# MAGIC 1. array_contains
# MAGIC 2. explode and flatten

# COMMAND ----------

# MAGIC %md
# MAGIC ###### array_contains

# COMMAND ----------

# array_contains 
'''
returns null if the array is null, true if the array contains the given value, and false otherwise.
'''
df4 = spark.createDataFrame([ (["a", "b", "c"],[1,2],), ([],[3,]) ], ['data','integers'])
df4.select(f.array_contains('data', '')).show()
df4.select(f.array_contains('data', 'b')).show()
df4.select(f.array_contains('integers', 1)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Explode

# COMMAND ----------

# Returns a new row for each element in the given array or map. Uses the default column name col for elements in the array and key and value for elements in the map unless specified otherwise.
# explode
from pyspark.sql import Row
import pyspark.sql.functions as f
df5 = spark.createDataFrame(
  [Row(a=1, intlist=[1,2,3], mapfield={"a": "b"}), Row(a=2, intlist=[5,6,7], mapfield={"c": "d"})]
)
df5.show()
df5.select(f.explode(f.col('intlist'))).collect()[0].asDict()
df5.select(f.explode(f.col('mapfield')).alias("KEY","VAL")).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Flatten

# COMMAND ----------

# flatten
'''Collection function: creates a single array from an array of arrays. If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.'''
df3 = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])
df3.select(f.flatten(df3.data).alias('r')).collect()

# COMMAND ----------

# DBTITLE 1,Sorting functions
# MAGIC %md
# MAGIC 1. Order by (ascending and descending)
# MAGIC 2. Sorting with null handling

# COMMAND ----------

#Reading dataset
fp = "/databricks-datasets/bikeSharing/data-001/day.csv"
df2 = spark.read\
  .format("csv")\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(fp)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### orderBy

# COMMAND ----------

# order by
df2.filter(f.col('mnth') == 6).orderBy(f.desc('windspeed')).show()
df2.filter(f.col('mnth') == 6).orderBy(f.asc('windspeed')).show()
df2.filter(f.col('mnth') == 6).orderBy('windspeed').show()
# sorting with null handling
df2.sort(f.col("temp")).show(5)
df2.sort(f.col("temp").asc_nulls_first()).show(5)
df2.sort(f.col("temp").desc_nulls_first()).show(5)

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC 
# MAGIC Aggregate Function Exercises
# MAGIC 1. Try sumDistinct() a data frame.
# MAGIC 2. Try rounding off the result of sum() to 3 decimal places.
# MAGIC 3. Try getting the max() out of the totals( sum() ) of all the columns of the data frame.
# MAGIC 4. Try applying max() & min() on the count of each column available in data frame.

# COMMAND ----------

'''
Write code here
'''

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [Spark Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

# COMMAND ----------

