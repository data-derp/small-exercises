# Databricks notebook source
# MAGIC %md
# MAGIC # Non-Aggregate Functions
# MAGIC 
# MAGIC In this notebook, we'll cover a few non-aggregate functions such as:
# MAGIC 1. Array Creation
# MAGIC 2. Null Handling
# MAGIC 3. Collection Functions
# MAGIC 4. Sorting Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

fl = "/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv"
df = spark.read\
  .format('csv')\
  .option('inferSchema', True)\
  .option('header', True)\
  .option('delimiter', ',')\
  .load(fl)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array Creation

# COMMAND ----------

df.take(1) # array of first row

# COMMAND ----------

v2 = df.take(5) # array of the first 5 rows

print("Array of rows: ", v2)
print("Length of the array: ", len(v2))
print("Type: ", type(v2))
print("0th element of the array: ", v2[0])
print("Type of the 0th element: ", type((v2[0])))
print("Functions of the 0th element: ", dir(v2[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Handling
# MAGIC 1. Creating an array
# MAGIC 2. `na.drop`, `na.fill` and `na.replace`

# COMMAND ----------

# MAGIC %md
# MAGIC ### na.drop
# MAGIC ```
# MAGIC drop(how='any', thresh=None, subset=None)
# MAGIC ```
# MAGIC 
# MAGIC Drop takes a `how` values of `any` (default, drop a row if it contains NULLs on any columns) or `all` (drop a row only if all columns have NULL values).
# MAGIC 
# MAGIC [Read more](https://sparkbyexamples.com/pyspark/pyspark-drop-rows-with-null-values/)

# COMMAND ----------

df.na.drop().count()

# COMMAND ----------

df.na.drop('all').count()

# COMMAND ----------

df.na.drop('any').count() #default

# COMMAND ----------

df.na.drop('any', 2) # atleast 2 values must be null

# COMMAND ----------

df.na.drop('any', 2, subset=['infected_by','symptom_onset_date']) # consider specific list of cols to check for nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ### na.fill
# MAGIC This function fills nulls with a specified value

# COMMAND ----------

df.na.fill('use this string to fill up all NULLs throughout').count()

# COMMAND ----------

df.na.fill('use this string to fill up NULLS from specific cols', subset=['infected_by','symptom_onset_date']).show() # consider specific list of cols

# COMMAND ----------

# MAGIC %md
# MAGIC ### na.replace
# MAGIC This function replaces a specific value with another value

# COMMAND ----------

df.select(f.col("state")).na.replace("released","Released").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collection Functions
# MAGIC 
# MAGIC 1. array_contains
# MAGIC 2. explode and flatten

# COMMAND ----------

# MAGIC %md
# MAGIC ### array_contains
# MAGIC This function returns `null` if the array is `null`, `true` if the array contains the given value, and `false` otherwise.

# COMMAND ----------

df4 = spark.createDataFrame([ (["a", "b", "c"],[1,2],), ([],[3,]) ], ['data','integers'])
display(df4)

# COMMAND ----------

df4.select(f.array_contains('data', '')).show()

# COMMAND ----------

df4.select(f.array_contains('data', 'b')).show()

# COMMAND ----------

df4.select(f.array_contains('integers', 1)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode
# MAGIC This function returns a new row for each element in the given array or map. It uses the default column name col for elements in the array and key and value for elements in the map unless specified otherwise.

# COMMAND ----------

from pyspark.sql import Row
import pyspark.sql.functions as f

df5 = spark.createDataFrame(
  [Row(a=1, intlist=[1,2,3], mapfield={"a": "b"}), Row(a=2, intlist=[5,6,7], mapfield={"c": "d"})]
)

df5.show()

# COMMAND ----------

df5.select(f.explode(f.col('intlist'))).show()

# COMMAND ----------

df5.select(f.explode(f.col('intlist')), f.col('mapfield')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flatten
# MAGIC This function creates a single array from an array of arrays. For example, if a structure of nested arrays is deeper than two levels, only one level of nesting is removed.

# COMMAND ----------

df3 = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])
df3.show()


# COMMAND ----------

df3.select(f.flatten(df3.data).alias('flattened')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting Functions
# MAGIC 1. Order by (ascending and descending)
# MAGIC 2. Sorting with null handling

# COMMAND ----------

file_path = "/databricks-datasets/bikeSharing/data-001/day.csv"
df2 = spark.read\
  .format("csv")\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(file_path)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### orderBy

# COMMAND ----------

df2.filter(f.col('mnth') == 6).orderBy(f.desc('windspeed')).show()

# COMMAND ----------

df2.filter(f.col('mnth') == 6).orderBy(f.asc('windspeed')).show()


# COMMAND ----------

df2.filter(f.col('mnth') == 6).orderBy('windspeed').show()

# COMMAND ----------

df2.sort(f.col("temp")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC There are additional helpful functions like
# MAGIC * [asc_nulls_first](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.asc_nulls_first.html)
# MAGIC * [desc_nulls_first](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.desc_nulls_first.html)