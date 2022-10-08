# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Functions
# MAGIC Being able to transform and wrangle data is an important part of Data Engineering. Let's have a look at some useful functions:
# MAGIC 
# MAGIC 1. Actions (show, collect, take)
# MAGIC 2. Row and Column functions
# MAGIC 3. Basic Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actions
# MAGIC Actions are triggers that tell Spark driver process to run the queries, aggregate the results and return them to client application. Some examples:
# MAGIC 
# MAGIC * show
# MAGIC * collect
# MAGIC * take

# COMMAND ----------

# MAGIC %md
# MAGIC 💡_Spark **lazily evaluates** queries. Until explicitly told to collect the result of the queries (using actions), Spark will not execute the queries_

# COMMAND ----------

file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
data = spark.read.parquet(file_path)
display(data)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Show
# MAGIC Shows the content of the DataFrame but in a tabular format. Let's filter the `room_type` column by `Private room` (don't forget to trim the whitespace).

# COMMAND ----------

from pyspark.sql.functions import col, trim

data.filter(trim(col("room_type")) == 'Private room').limit(2).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Collect
# MAGIC The collect retrieves data from a driver node in the form of an Array of Rows (it additionally shows some metadata). Let's filter the `room_type` column by `Private room` (don't forget to trim the whitespace).
# MAGIC 
# MAGIC [Read more here](https://sparkbyexamples.com/pyspark/pyspark-collect/)

# COMMAND ----------

from pyspark.sql.functions import col, trim

data.filter(trim(col("room_type")) == 'Private room').limit(2).collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Take
# MAGIC Let's `take` the first N number of results from the DataFrame. It shows content and structure/metadata for a limited number of rows for a very large dataset. 

# COMMAND ----------

data.take(num=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rows and Column Functions
# MAGIC 
# MAGIC 1. Columns
# MAGIC 2. Rows
# MAGIC 3. Adding and renaming column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columns

# COMMAND ----------

from pyspark.sql.functions import col

display(data.select(col("room_type")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rows
# MAGIC Rows are records in the DataFrame

# COMMAND ----------

data.first()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding and renaming a column

# COMMAND ----------

# MAGIC %md
# MAGIC Let's add a new column to the DataFrame called `is_apartment` for listings that are an `Apartment`. If the `property_type` is equal to `Apartment`, `is_apartment` should be `True`, else `False`.
# MAGIC 
# MAGIC **Hint** use [when](https://sparkbyexamples.com/pyspark/pyspark-when-otherwise/) as the if statement

# COMMAND ----------

from pyspark.sql.functions import col, when

new_df=data.withColumn("is_apartment", when(trim(col("property_type")) == 'Apartment', "True").otherwise("False"))

# COMMAND ----------

display(new_df.select("property_type","is_apartment"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Transformations
# MAGIC 1. Select
# MAGIC 2. Filter
# MAGIC 3. Casting
# MAGIC 4. Distinct
# MAGIC 5. Limit
# MAGIC 6. Aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC Let's pull in three types of data, all of different formats

# COMMAND ----------

iot_devices_path = '/databricks-datasets/iot/iot_devices.json'
iot_devices = spark.read.json(iot_devices_path)

# COMMAND ----------

sf_airbnb_path = '/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet/'
sf_airbnb = spark.read.parquet(sf_airbnb_path)

# COMMAND ----------

sf_fire_calls_path = '/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv'
sf_fire_calls =  spark.read\
  .option('inferSchema', False)\
  .option('header', True)\
  .csv(sf_fire_calls_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select
# MAGIC Return a DataFrame with the columns: `neighbourhood_cleansed`, `property_type`, and `accommodates`

# COMMAND ----------

display(sf_airbnb.select('neighbourhood_cleansed', 'property_type', 'accommodates'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Filter
# MAGIC Return a Dataframe with listings where the `property_type` is `Apartment`, the `minimum_nights` is `1`, and `accomodates` is `greater than 3`.

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

filtered_df = sf_airbnb.filter("property_type='Apartment'").filter('minimum_nights=1').filter('accommodates>3')
display(filtered_df)

# COMMAND ----------

print(filtered_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casting
# MAGIC Casting allows us to convert an entire column of data to our desired datatype.

# COMMAND ----------

sf_fire_calls.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that all columns are of type `string` despite there being columns which might more sense as an `integer`, for example `Call Number`. Let's cast this one to `integer`.

# COMMAND ----------

from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

sf_fire_calls.select(F.col('Call Number').cast(IntegerType())).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct

# COMMAND ----------

iot_devices.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's find distinct lcd values.

# COMMAND ----------

iot_devices.select('lcd').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit
# MAGIC Limits the number of results

# COMMAND ----------

display(sf_fire_calls.select('*').limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate

# COMMAND ----------

from pyspark.sql.functions import avg, col
avg_price_groupedby_review = data.groupBy("review_scores_rating").agg(avg("price").alias("Average price")).sort("review_scores_rating")
display(avg_price_groupedby_review)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise
# MAGIC 
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