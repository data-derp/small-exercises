# Databricks notebook source
# MAGIC %md
# MAGIC # Caching
# MAGIC 
# MAGIC If you plan to use the same dataset over and over for some tranformations the most simple opimization that you can go for is **caching**. There are different levels of caching:
# MAGIC 1. **DISK_ONLY**: Persist data on disk only in serialized format.
# MAGIC 2. **MEMORY_ONLY**: Persist data in memory only in deserialized format.
# MAGIC 3. **MEMORY_AND_DISK**: Persist data in memory and if enough memory is not available evicted blocks will be stored on disk.
# MAGIC 4. **OFF_HEAP**: Data is persisted in off-heap memory. Refer spark.memory.offHeap.enabled in the Spark Documentation.
# MAGIC 
# MAGIC **storageLevel** in the DataFrame API gives the storage information about the DataFrame in the format:
# MAGIC 
# MAGIC ```
# MAGIC (
# MAGIC   boolean useDisk
# MAGIC   boolean useMemory
# MAGIC   boolean useOffHeap
# MAGIC   boolean deserialized
# MAGIC   int replication
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC [See how the combination of values results in the different levels of caching](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/storage/StorageLevel.scala#L148-L161)
# MAGIC 
# MAGIC [Read more about caching](https://towardsdatascience.com/apache-spark-caching-603154173c48#:~:text=Caching%20methods%20in%20Spark&text=DISK_ONLY%3A%20Persist%20data%20on%20disk,persisted%20in%20off%2Dheap%20memory.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the notebook

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

file_path = 'dbfs:/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet/'
data = spark.read.parquet(file_path)

display(data)

# COMMAND ----------

data.storageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC Caching is lazy operation, which means the data will not be cached until the first action operation is performed on it. Therefore, we must performan an action to apply this. The action will trigger the caching.

# COMMAND ----------

data.cache()

# the boolean value for useDisk, useMemory should be true and default caching mechanism is MEMORY_AND_DISK
data.storageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uncache previously cached data
# MAGIC Let's [unpersist](https://spark.apache.org/docs/latest/api/python//reference/pyspark.sql/api/pyspark.sql.DataFrame.unpersist) the data that is cached previously to free up Memory after doing our operation. Think of it as manual Garbage Collection

# COMMAND ----------

data.unpersist(blocking=True)
data.storageLevel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert a DataFrame to a Global or Temporary View
# MAGIC Similar to SQLm you can create views out of your datasets. Let's create a temporary view of all the listings with >90 percent score or has flexible cancellation policy.

# COMMAND ----------

from pyspark.sql import functions as f

temp_view = data.filter((f.col("cancellation_policy") == "flexible") | (f.col("review_scores_rating") > 90))
temp_view.createOrReplaceTempView('tempView')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tempView LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Global View
# MAGIC Global views exist across the SparkSessions.

# COMMAND ----------

temp_view.createOrReplaceGlobalTempView('globalTempView')

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _Try accessing the global temporary view by attaching another notebook to this cluster_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Hints
# MAGIC We can provide execution hints to spark that can help run the code in more optimized way. For example we are going to learn more about **Broadcast** operation. Before we get started, let's load a couple of datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up data

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/nyctaxi/tripdata/

# COMMAND ----------

# MAGIC %md
# MAGIC Let's load a large table. It'll take a while, so please be patient!

# COMMAND ----------

large_table = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz"

large_df = spark.read\
  .format('csv')\
  .option('header', True)\
  .option('inferSchema', True)\
  .load(large_table)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll also load a smaller table

# COMMAND ----------

small_table = "/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv"

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
# MAGIC ### Broadcast
# MAGIC During a Join, Spark performs either of the two, 1) shuffle join or 2) broadcast join
# MAGIC 1. **Shuffle join**: When data has to be shuffled between each node so that partitions with matching join "key" reside on the same node. 
# MAGIC 2. **Broadcast join**: When the size of one DataFrame is small enough we can instruct Spark to send all paritions of that DataFrame to each of the nodes where partitions of the Large DataFrame reside. This would generally have lesser overhead than a shuffle join.

# COMMAND ----------

# MAGIC %md
# MAGIC **How many users paid with credit cards?** To answer this question, we'll need to join the large and small DataFrames. We'll tell Spark that we would like to **broadcast** the small DataFrame which will send all of its partitions to each node.

# COMMAND ----------

from pyspark.sql import functions as f

count_credit_card_payers = large_df.alias('large').join(f.broadcast(small_df.alias('small')), f.col('large.payment_type')==f.col('small.payment_type')).filter(f.col("small.payment_desc") == 'Credit card').count()

print(count_credit_card_payers)