# Databricks notebook source
# MAGIC %md
# MAGIC 💡 _Run the following command to initalise data setup and library imports_

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

# MAGIC %run ./init_data

# COMMAND ----------

# MAGIC %md 
# MAGIC The DataFrame reader API can be accessed through the SparkSession using the following API
# MAGIC 
# MAGIC ```
# MAGIC spark.read.format(...)
# MAGIC   .option('mode','...')
# MAGIC   .option('inferSchema', '...')
# MAGIC   .option('path', '...')
# MAGIC   .schema(...)
# MAGIC   .load()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Read data for the “core” data formats (CSV, JSON, JDBC, ORC, Parquet, text and tables)
# MAGIC %md
# MAGIC Spark has 6 core data sources for which out of the box reading capability is provided:
# MAGIC 1. CSV: Comma seperated values
# MAGIC 2. JSON: JavaScipt Object Notation
# MAGIC 3. Parquet: An open source column oriented data storage format
# MAGIC 4. Plain/text file
# MAGIC 5. ORC: An open source column oriented data storage format
# MAGIC 6. JDBC: Database connector
# MAGIC 
# MAGIC We will explore reading the data for some of these

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Reading CSV

# COMMAND ----------

# Reading 
csv_data_path = '/databricks-datasets/bikeSharing/data-001/day.csv'
atlas_csv = '/databricks-datasets/atlas_higgs/atlas_higgs.csv'
tsv_powerplant = '/databricks-datasets/power-plant/data/Sheet1.tsv'
credit_card_parquet = '/databricks-datasets/credit-card-fraud/data'
parquet_data_path = '/databricks-datasets/airlines/'


# COMMAND ----------

# reading csv 
csv_df = spark.read\
  .csv('/databricks-datasets/bikeSharing/data-001/day.csv')
display(csv_df)

# COMMAND ----------

csv_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 💡  _The CSV file read above does not take into account first row as header and DataTypes of all the column are Strings_

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Reading the parquet file

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/amazon/test4K

# COMMAND ----------

#reading parquet 
parquet_df = spark.read\
  .parquet('/databricks-datasets/amazon/test4K')
parquet_df.printSchema()

# COMMAND ----------

display(parquet_df)

# COMMAND ----------

# DBTITLE 1,How to read data from non-core formats using format() and load()
'''
Let's read a tab seperated values or tsv file using the format()
and load() methods. 
'''

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

# DBTITLE 1,How to configure options for specific formats
# MAGIC %fs
# MAGIC head /databricks-datasets/bikeSharing/data-001/day.csv

# COMMAND ----------

'''
Reading the a CSV file
'''
file_path = '/databricks-datasets/bikeSharing/data-001/day.csv'

df = spark.read.csv(file_path)

# COMMAND ----------

'''
printSchema method displays the Schema of the dataFrame. You get to see the 
column names and Datatypes among other information
'''
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the headers for the file were not read from the first row and the dataTypes are all strings

# COMMAND ----------

display(df)

# COMMAND ----------

'''
Reading the file again but this time giving an "Option" to read  the 'header', and
intelligently infer the schema ('inferSchema') i.e. determine the DataType automatically
'''
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

# DBTITLE 1,How to specify a DDL-formatted schema
# MAGIC %md
# MAGIC Spark also has a provision for the user to provide DDL like syntax for declaration of schema

# COMMAND ----------

file_path = '/databricks-datasets/iot/iot_devices.json'
data = spark.read.format("csv").load(file_path).toPandas()

# COMMAND ----------

'''
Providing explicit schema declaration
'''
spark.conf.set("spark.sql.legacy.charVarcharAsString","true")

file_path = '/databricks-datasets/iot/iot_devices.json'
#create the custom SQL like schema declaration
CustomSchema = 'device_id INT, ip VARCHAR(20), cca3 varchar(3), latitude decimal, longitude double'

df = spark.read\
  .schema(CustomSchema)\
  .json(file_path)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,How to construct and specify a schema using the StructType classes
# MAGIC %md
# MAGIC Spark provides great flexibility in reading unstructured data using the inbuilt **StructType class** and custom schema declaration. A StructType can have multiple **StructField type** objects with differing datatypes.

# COMMAND ----------

#make sure the first command has been run

#reading JSON file
file_path = f'/FileStore/{current_user}/wrangling-with-spark/student.json'

df = spark.read\
  .json(file_path)

#Examining Schema
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of the example, say we need to create a DataFrame out of this dataSet that contains student id, name and subjects

# COMMAND ----------

#import different Spark DataTypes 
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType, ArrayType

#Define the schema of resultant dataFrame
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

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC **Scenario 5.1**
# MAGIC <br>
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
# MAGIC **Scenario 5.2**
# MAGIC <br>
# MAGIC **Dataset**: dbfs:/FileStore/YOUR_USERNAME/wrangling-with-spark/student.json (**Hint:** interpolate the variable `current_user` in place of `YOUR_USERNAME`)
# MAGIC <br>
# MAGIC **Problem statement**: Read the JSON file into the following schema
# MAGIC ###### root
# MAGIC ######  |-- id: double (nullable = true)
# MAGIC ######  |-- name: string (nullable = true)
# MAGIC ######  |-- subjects: array (nullable = true)
# MAGIC ######  |&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;|-- element: string (containsNull = true)
# MAGIC ######  |-- total_marks: float (nullable = true)
# MAGIC ######  |-- address: struct (nullable = true)
# MAGIC ######  |&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;|-- city: string (nullable = true)
# MAGIC <br>
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

# COMMAND ----------

