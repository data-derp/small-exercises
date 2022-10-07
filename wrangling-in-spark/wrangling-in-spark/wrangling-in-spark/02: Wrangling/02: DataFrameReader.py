# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrameReader
# MAGIC 
# MAGIC In order to work with our data in Spark, we must read data into Spark. One such method is using the **DataFrameReader**.
# MAGIC 
# MAGIC Spark has 6 core data sources for which out of the box reading capability is provided:
# MAGIC 1. **CSV**: Comma seperated values
# MAGIC 2. **JSON**: JavaScipt Object Notation
# MAGIC 3. **Parquet**: An open source column oriented data storage format
# MAGIC 4. **Plain/text file**
# MAGIC 5. **ORC**: An open source column oriented data storage format
# MAGIC 6. **JDBC**: Database connector
# MAGIC 
# MAGIC The **DataFrameReader** API can read these core data sources through the SparkSession using the following API:
# MAGIC ```
# MAGIC spark.read.format(...)
# MAGIC   .option('mode','...')
# MAGIC   .option('inferSchema', '...')
# MAGIC   .option('path', '...')
# MAGIC   .schema(...)
# MAGIC   .load()
# MAGIC ```
# MAGIC 
# MAGIC In this notebook, we'll will demonstrate the following:
# MAGIC 1. Read a CSV file
# MAGIC 2. Read a parquet file
# MAGIC 3. Read a non-standard format using format() and load()
# MAGIC 4. Configure options for specific formats
# MAGIC 5. Specify a DDL-formatted schema
# MAGIC 6. Construct and specify a schema using StructType classes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %run ../init_data

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Reading a CSV file

# COMMAND ----------

csv_data_path = '/databricks-datasets/bikeSharing/data-001/day.csv'
atlas_csv = '/databricks-datasets/atlas_higgs/atlas_higgs.csv'
tsv_powerplant = '/databricks-datasets/power-plant/data/Sheet1.tsv'
credit_card_parquet = '/databricks-datasets/credit-card-fraud/data'
parquet_data_path = '/databricks-datasets/airlines/'


# COMMAND ----------

csv_df = spark.read\
  .csv('/databricks-datasets/bikeSharing/data-001/day.csv')
display(csv_df)

# COMMAND ----------

csv_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 💡  _The CSV file read above does not take into account first row as header and DataTypes of all the column are Strings. We'll demonstrate how to solve that later in this notebook._

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a parquet file

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/amazon/test4K

# COMMAND ----------

parquet_df = spark.read\
  .parquet('/databricks-datasets/amazon/test4K')
parquet_df.printSchema()

# COMMAND ----------

display(parquet_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading non-standard formats using format() and load()
# MAGIC ... for example a tab separated value (tsv) file

# COMMAND ----------

# DBTITLE 0,How to read data from non-core formats using format() and load()
file_path = '/databricks-datasets/power-plant/data/Sheet1.tsv'
df = spark.read\
  .format('csv')\
  .option('delimiter', '\t')\
  .option('header', True)\
  .load(file_path)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure options for specific formats
# MAGIC Sometimes you'll need to specify some additional parameters in order to properly read in a file

# COMMAND ----------

# DBTITLE 0,How to configure options for specific formats
# MAGIC %fs
# MAGIC head /databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

file_path = '/databricks-datasets/bikeSharing/data-001/day.csv'

df = spark.read.csv(file_path)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the headers for the file were **not** read from the first row and the dataTypes are all strings

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's read the file again but this time giving an "Option" to read  the 'header', and intelligently infer the schema ('inferSchema') i.e. determine the DataType automatically

# COMMAND ----------

file_path = '/databricks-datasets/bikeSharing/data-001/day.csv'

df = spark.read\
  .option('header', True)\
  .option('inferSchema', True)\
  .csv(file_path)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 0,How to specify a DDL-formatted schema
# MAGIC %md
# MAGIC ## Specify a DDL-formatted schema
# MAGIC Spark also has a provision for the user to provide DDL like syntax for declaration of schema

# COMMAND ----------

file_path = '/databricks-datasets/iot/iot_devices.json'
data = spark.read.format("csv").load(file_path).toPandas()

# COMMAND ----------

spark.conf.set("spark.sql.legacy.charVarcharAsString","true")

file_path = '/databricks-datasets/iot/iot_devices.json'

CustomSchema = 'device_id INT, ip VARCHAR(20), cca3 varchar(3), latitude decimal, longitude double'

df = spark.read\
  .schema(CustomSchema)\
  .json(file_path)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 0,How to construct and specify a schema using the StructType classes
# MAGIC %md
# MAGIC ## Construct and specify a schema using StructType classes
# MAGIC Spark provides great flexibility in reading unstructured data using the inbuilt **StructType class** and custom schema declaration. A StructType can have multiple **StructField type** objects with differing datatypes.

# COMMAND ----------

file_path = f'/FileStore/{current_user}/wrangling-with-spark/student.json'

df = spark.read\
  .json(file_path)

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of the example, say we need to create a DataFrame out of this dataSet that contains student id, name and subjects

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType, ArrayType

df_schema = StructType([
  StructField('id', DoubleType(), True),
  StructField('name', StringType(), True),
  StructField('subjects', ArrayType(StringType()), True)
])

file_path = f'/FileStore/{current_user}/wrangling-with-spark/student.json'
df = spark.read\
  .schema(df_schema)\
  .json(file_path)
df.printSchema()


# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 0,Exercise
# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC **Dataset**: dbfs:/FileStore/YOUR_USERNAME/wrangling-with-spark/df_some_sep (**Hint:** interpolate the variable `current_user` in place of `YOUR_USERNAME`)
# MAGIC <br>
# MAGIC **Problem statement**: Read the dataset into a DataFrame using a custom seperator
# MAGIC <br>
# MAGIC **Steps to follow**:
# MAGIC 1. Peek in to the dataset using the "dbutils.fs.head()" command
# MAGIC 2. Determine the type of delimiter
# MAGIC 3. Read the file using the correct options

# COMMAND ----------

'''
Write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

dbutils.fs.head(f'{working_directory}/df_some_sep')

# COMMAND ----------

file_path = f'{working_directory}/df_some_sep'

df = spark.read\
  .option('delimiter', '$')\
  .option('header', "true")\
  .option("mode", "DROPMALFORMED")\
  .option('inferSchema', "true")\
.csv(file_path)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC **Dataset**: dbfs:/FileStore/YOUR_USERNAME/wrangling-with-spark/student.json (**Hint:** interpolate the variable `current_user` in place of `YOUR_USERNAME`)
# MAGIC <br>
# MAGIC **Problem statement**: Read the JSON file into the following schema
# MAGIC 
# MAGIC <pre>
# MAGIC  root
# MAGIC  |-- id: double (nullable = true)
# MAGIC  |-- name: string (nullable = true)
# MAGIC  |-- subjects: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC  |-- total_marks: float (nullable = true)
# MAGIC  |-- address: struct (nullable = true)
# MAGIC </pre>
# MAGIC 
# MAGIC **Steps to follow**:
# MAGIC 1. Peek in to the dataset using the "head" command
# MAGIC 2. Define the schema using StructType and StructField classes. Hint: StructFeild's can be nester
# MAGIC 3. Read the file using the correct options

# COMMAND ----------

'''
Write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

# read the 'student.json' file into a
# schema given below:
# root
#  |-- id: double (nullable = true)
#  |-- name: string (nullable = true)
#  |-- subjects: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- total_marks: float (nullable = true)
#  |-- address: struct (nullable = true)
#  |    |-- city: string (nullable = true)

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, ArrayType, FloatType

df_schema = StructType([
  StructField('id', DoubleType(), True),
  StructField('name', StringType(), True),
  StructField('subjects', ArrayType(StringType()), True),
  StructField('total_marks', FloatType(), True),
  StructField('address', StructType([
    StructField('city', StringType(), True)
  ]), True)
])

file_path = f'{working_directory}/student.json'
df = spark.read\
  .schema(df_schema)\
  .json(file_path)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [DataFrameReader](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrameReader)
# MAGIC 2. [StructType](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.types.StructType)
# MAGIC 3. [StructField](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.types.StructField)
# MAGIC 4. [Parquet file format](https://en.wikipedia.org/wiki/Apache_Parquet)
# MAGIC 5. [Handling Malformed CSVs](https://medium.com/@smdbilal.vt5815/csv-bad-record-handling-and-its-complications-pyspark-f3b871d652ba)