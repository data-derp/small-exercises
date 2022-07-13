# Databricks notebook source
# DBTITLE 1,How to cache data
# MAGIC %md
# MAGIC If you plan to use the same dataset over and over for some tranformations the most simple opimization that you can go for is **caching**. There are different levels of caching:
# MAGIC 1. MEMORY_ONLY
# MAGIC 2. MEMORY_AND_DISK (Default)
# MAGIC 3. DISK_ONLY
# MAGIC 4. OFF_HEAP

# COMMAND ----------

#read the file
file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
cache_data = spark.read.parquet(file_path)

display(cache_data)

# COMMAND ----------

# MAGIC %md 
# MAGIC 💡 **storageLevel** gives the storage information about the dataframe
# MAGIC <br>_(boolean useDisk, boolean useMemory, boolean useOffHeap, boolean deserialized, int replication)_

# COMMAND ----------

cache_data.storageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC 💡  _Caching is lazy operation, which means the data will not be cached until the first action operation is performed on it_

# COMMAND ----------

'''
As this operation is lazy we must perform an action to
apply this, this action will take time as caching of the dataset in also implicit
'''
cache_data.cache()

# the boolean value for useDisk,  useMemory should be true and Defualt caching mechanism is MEMORY_AND_DISK
cache_data.storageLevel

# COMMAND ----------

# DBTITLE 1,Uncache previously cached data
'''
We will unpersist the data that is cached previously to free up Memory after doing our operation
Think of it as manual Garbage Collection
'''
cache_data.unpersist(blocking=True)
cache_data.storageLevel

# COMMAND ----------

# DBTITLE 1,Converting a DataFrame to a global or temp view.
'''
As in SQL you can create views out of your datasets upon appliying some transformation 
or projection.
'''
from pyspark.sql import functions as f
# creating a temp view of all the listings with >90 percent score or flexible cancellation policy
temp_view = cache_data.filter((f.col("cancellation_policy") == "flexible") | (f.col("review_scores_rating") > 90))
temp_view.createOrReplaceTempView('tempView')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tempView LIMIT 10;

# COMMAND ----------

'''
Global views exist throughout the spark session.
'''
temp_view.createOrReplaceGlobalTempView('globalTempView')

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Try accessing the global temporary view by attaching another notebook to this cluster_

# COMMAND ----------

# DBTITLE 1,Applying hints
# MAGIC %md
# MAGIC We can provide execution hints to spark that can help run the code in more optimized way. For example we are going to learn more about 'Broadcast' operation.

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/nyctaxi/tripdata/

# COMMAND ----------

small_table = "/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv"
large_table = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz"

# This command is going to take a very long time as we are reading a big dataset, be patient !!
large_df = spark.read\
  .format('csv')\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(large_table)

# COMMAND ----------

#Let us now load a small dataset
small_df = spark.read\
  .format('csv')\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(small_table)


# COMMAND ----------

print(f"rows in small dataFrame: {small_df.count()}")
print(f"rows in large dataFrame: {large_df.count()}")

# COMMAND ----------

display(small_df)

# COMMAND ----------

display(large_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### What is Broadcast?

# COMMAND ----------

# MAGIC %md
# MAGIC During a Join, Spark performs either of the two, 1) shuffle join or 2) Broadcast join
# MAGIC 1. **Shuffle join**: When data has to be shuffled between each node so that partitions with matching join "key" reside on the same node. 
# MAGIC 2. **Broadcast join**: When the size of one DataFrame is small enough we can instruct Spark to send all paritions of that data frame to each of the nodes where partitions of the _Big DataFrame_ reside. This would generally have lesser overhead than a shuffle join.

# COMMAND ----------

'''
To answer the question, how many users paid with credit cards, we will have to do join of large dataframe with small data frame
'''
from pyspark.sql import functions as f
#Let's do a join and give spark a hint about broadcasting it
count_credit_card_payers = large_df.alias('large').join(f.broadcast(small_df.alias('small')), f.col('large.payment_type')==f.col('small.payment_type')).filter(f.col("small.payment_desc") == 'Credit card').count()
#Get the results
print(count_credit_card_payers)

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC 1. [unpersist function](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.unpersist)

# COMMAND ----------

