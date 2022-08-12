# Databricks notebook source
# DBTITLE 1,Using action such as take(), collect(), show() etc.
# MAGIC %md
# MAGIC Actions are triggers that tell Spark driver process to run the queries, aggregate the results and return them to client application. Following are some action commands:
# MAGIC 1. take
# MAGIC 2. Collect
# MAGIC 3. show

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

# MAGIC %md
# MAGIC 💡_Spark works on **lazy evaluation** of queries. Until explicitly told to collect the result of the queries (using actions), Spark will not execute the queries_

# COMMAND ----------

'''
Using display to display the dataframe. By now you must be familiar with this function !
'''
file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
data = spark.read.parquet(file_path)
display(data)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Take

# COMMAND ----------

'''
take first N number of results from the dataframe
'''
data.take(num=2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Collect

# COMMAND ----------

'''
collect action takes the output of transformation and projections
'''
from pyspark.sql.functions import col, trim

# Select rows with room_type Private room
# Make sure to Trim extra spaces from the columns
# Limit output to only 2 results
# Show the output

data.filter(trim(col("room_type")) == 'Private room').limit(2).collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Show

# COMMAND ----------

'''
Same result as above but using the "show" action
'''

# Select rows with room_type Private room
# Make sure to Trim extra spaces from the columns
# Limit output to only 2 results
# Show the output
data.filter(trim(col("room_type")) == 'Private room').limit(2).show()

# COMMAND ----------

# DBTITLE 1,Rows and Columns
# MAGIC %md
# MAGIC 1. Columns
# MAGIC 2. Rows
# MAGIC 3. Adding and renaming column

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Columns

# COMMAND ----------

'''
Columns are same as they are in spreadsheet/Pandas DataFrame
'''

# Reading dataset
file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
data = spark.read.parquet(file_path)

#Import functions that enable access to columns
from pyspark.sql.functions import col

display(data.select(col("room_type")))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Rows

# COMMAND ----------

'''
Rows in Spark are records of the DataFrame
'''

# Reading dataset
file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
data = spark.read.parquet(file_path)

#fetching the first row from the DataFrame
data.first()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Adding and renaming column

# COMMAND ----------

'''
Let's add a column to the DataFrame using the native functions "withColumn"
'''
# Reading dataset
file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
data = spark.read.parquet(file_path)
#display data
display(data)

# COMMAND ----------

'''
Let's add a new column to the DataFrame "is_apartment_or_not" to check if the listing
is an "Apartment" or not
'''

# importing necessary 
from pyspark.sql.functions import col, when

# Add a column "is_apartment_or_not" with entry True is the "property_type" is apartment
# else return False
new_df=data.withColumn("is_apartment_or_not", when(col("property_type") == 'Apartment', "True").otherwise("False"))

# COMMAND ----------

display(new_df.select("property_type","is_apartment_or_not"))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _when and otherwise are Spark syntax to write if-else statement_ 

# COMMAND ----------

# DBTITLE 1,Transformations
# MAGIC %md
# MAGIC  
# MAGIC 1. Selecting and filtering
# MAGIC 2. Casting into datatypes
# MAGIC 2. Repartitioning  and Coalescing
# MAGIC 4. Aggregation

# COMMAND ----------

#csv
sf_fire_calls = '/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv'
#parquet
sf_airbnb = '/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet/'
#json
iot_devices = '/databricks-datasets/iot/iot_devices.json'

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Selecting

# COMMAND ----------

#select or selectExpr

#read the data
df = spark.read\
  .parquet(sf_airbnb)

#select 'neighbourhood_cleansed', 'property_type', 'accommodates' columns 
display(df.select('neighbourhood_cleansed', 'property_type', 'accommodates'))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Filtering

# COMMAND ----------

#Filter

from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

# Let us get the count of 'Apartment'
# which 'accommodates' at least 3 people
# where 'minimum_nights' is 1
print(df.filter("property_type='Apartment'").filter('minimum_nights=1').filter('accommodates>3').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Casting

# COMMAND ----------

'''
Cast Columns

Read the dataset without inferring the
schema. This will allow us room to cast
the column to a datatype we want.
'''
cast_example_df = spark.read\
  .option('inferSchema', False)\
  .option('header', True)\
  .csv(sf_fire_calls)
cast_example_df.printSchema()

# COMMAND ----------

#importing the spark datatype
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

#cast the column 'Call Number' as integer
cast_example_df.select(F.col('Call Number').cast(IntegerType())).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Distinct

# COMMAND ----------

#Distinct
unique_example_df = spark.read.json(iot_devices)
unique_example_df.printSchema()

# COMMAND ----------

#Distinct

#let's find distinct lcd values
unique_example_df.select('lcd').distinct().show()

# COMMAND ----------

#Method: limit
#Usage: used to limit the number of responses
#example:
df = spark.read\
  .option('header', True)\
  .option('inferSchema', True)\
  .csv(sf_fire_calls)
df.select('*').limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Repartition and Coalesce

# COMMAND ----------

#Display the data
data = spark.read.parquet(sf_airbnb)
display(data)

# COMMAND ----------

from pyspark.sql import functions as f

data_partition = data.repartition(f.col('review_scores_value'))
# Select 
select_data = data_partition.select('review_scores_value', f.spark_partition_id().alias("pid")).distinct()
display(select_data)

# COMMAND ----------

data_repartition_4 = data.repartition(4,f.col('review_scores_value'))
select_data = data_repartition_4.select('review_scores_value', f.spark_partition_id().alias("pid")).distinct()
display(select_data)

# COMMAND ----------

#Coalesce
data_coalesce_2 = data_repartition_4.coalesce(2)
select_data = data_coalesce_2.select('review_scores_value', f.spark_partition_id().alias("pid")).distinct()
display(select_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Aggregation

# COMMAND ----------

'''
Deepdive in  Spark SQL Functions Part I
'''

#aggregation
data = spark.read.parquet(sf_airbnb)
display(data)

# COMMAND ----------

from pyspark.sql.functions import avg, col
avg_price_groupedby_review = data.groupBy("review_scores_rating").agg(avg("price").alias("Average price")).sort("review_scores_rating")
display(avg_price_groupedby_review)

# COMMAND ----------

# DBTITLE 1,Exercise
# MAGIC %md
# MAGIC **Scenario 7.1**
# MAGIC <br>**Dataset**: /databricks-datasets/learning-spark-v2/mnm_dataset.csv
# MAGIC <br>**Scenario**: What is the probability that a state gets greater than 55 blue MnM candy on average ?
# MAGIC <br>**Steps**:
# MAGIC 1. Read mnm dataset from following path: "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"
# MAGIC 2. Count distinct states (count_states)
# MAGIC 3. Filter the records by color of the MnM (Blue), group by state and then calculate average MnM count
# MAGIC 4. Filter again where "average_count" > 55 and count the number of records (count_states_w_gt_55_blue_mnm) 
# MAGIC 5. Calculate (count_states_w_gt_55_blue_mnm)/(count_states)

# COMMAND ----------

'''Write code here'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/learning-spark-v2/mnm_dataset.csv

# COMMAND ----------

file_path = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"
df = spark.read\
  .format('csv')\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(file_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, avg
count_states = df.select(col("State")).distinct().count()
print(f"count of states: {count_states}")
filter_by_blue = df.filter(col('Color') == "Blue" )
groupBy_state_avg_count = filter_by_blue.groupBy(col('State')).agg(avg(col("Count")).alias('average_count'))
count_states_w_gt_55_blue_mnm = groupBy_state_avg_count.filter(col('average_count')>55).count()
print(f"count of states with greater than 55 blue MnM count: {count_states_w_gt_55_blue_mnm}")
probability = count_states_w_gt_55_blue_mnm / count_states
print(f"Probability: {probability}")

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [withColumn](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn)
# MAGIC 2. [when](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.functions.when)
# MAGIC 3. [otherwise](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.Column.otherwise)

# COMMAND ----------

