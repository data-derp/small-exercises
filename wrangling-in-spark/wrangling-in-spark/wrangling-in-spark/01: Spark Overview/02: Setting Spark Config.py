# Databricks notebook source
# DBTITLE 0,Spark Configuration
# MAGIC %md
# MAGIC # Setting Spark Configuration
# MAGIC Spark contains a lot of configurable properties in the following catagories:
# MAGIC 1. Application properties
# MAGIC 2. Runtime environment
# MAGIC 3. Shuffle behavior
# MAGIC 4. Execution behavior
# MAGIC 5. Compression and serialization etc.
# MAGIC 
# MAGIC In this demo, we'll demonstrate how to set Spark Configuration for (3) Shuffle Behaviour.

# COMMAND ----------

# MAGIC %run ../init

# COMMAND ----------

# MAGIC %md
# MAGIC Get all the properties by calling `.getConf().getAll()` on the Spark Context

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

# DBTITLE 1,Getting and Setting properties
df = spark.read\
  .parquet('/databricks-datasets/amazon/data20K')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's get and set the shuffle property and obeserve how the changes in the output

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is a shuffle? 
# MAGIC 
# MAGIC Data in spark is distributed over several nodes. In order to be efficient, Spark moves the data such that data with the same key end up on the same partition. This **data exchange is known as [shuffle](https://sparkbyexamples.com/spark/spark-shuffle-partitions/)**.
# MAGIC 
# MAGIC We often shuffle into partitions. Paritioning is a way to split data into multiple parts to achieve parallelism in order to enable a job to complete faster.

# COMMAND ----------

"""
By default spark partitions data into 200 parts
"""
print(spark.conf.get('spark.sql.shuffle.partitions'))

# COMMAND ----------

"""
Expand the Spark Job to see the partitions created in the last stage
of processing. You will notice that by default the shuffle partitions 
are '200'
"""
df.groupBy(df.rating).count().write.mode('overwrite').parquet(f'{working_directory}/tmp/test')

# COMMAND ----------

"""
Let's change the number of partitions that are used during that shuffle
using the 'spark.sql.shuffle.partitions' property.
"""
spark.conf.set('spark.sql.shuffle.partitions','100')
print(spark.conf.get('spark.sql.shuffle.partitions'))


# COMMAND ----------

"""
Now if you would expand the jobs you will see that the number of partitions
has reduced from '200' to '100'
"""
df.groupBy(df.rating).count().write.mode('overwrite').parquet(f'{working_directory}/tmp/test')