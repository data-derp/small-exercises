# Databricks notebook source
# MAGIC %md
# MAGIC 💡 _Starting Spark 2.0 SparkContext or SQLContext are encapsulated within the SparkSession_

# COMMAND ----------

# DBTITLE 1,SparkContext
# MAGIC %md
# MAGIC - When executing a Spark application, we must interact with the cluster using a SparkSession.
# MAGIC - Using SparkSession, you can access all of low-level and legacy contexts and configurations accordingly
# MAGIC - A SparkContext object within SparkSession represents connection with the spark cluster. This class is how you communicate with some of Spark’s lower-level APIs, such as RDDs. (SparkSession can have supports different contexts like HiveContext etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _What is RDD ?_<br>_Resilient distributed dataset or RDD is an abstraction that Spark provides over data distributed accross nodes. Think of it as a distributed data structure_

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

# MAGIC %run ./init_data

# COMMAND ----------

# DBTITLE 1,Using SparkContext
'''
SparkContext is available in Databricks environment using the "sc" variable
'''
sc

# COMMAND ----------

data = [1, 2, 3, 4, 5]
# Create data into a distributed data structure (RDD)
distData = sc.parallelize(data)
#let's print the type of the distributed data structure
print(type(distData))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡
# MAGIC _By default (e.g. with AQE turned off) sparkContext is creating the same number partitions as # cores per worker when creating RDD, you can check that by expanding 'Spark Jobs->Job ->Stage'_

# COMMAND ----------

# MAGIC %md 
# MAGIC ![Jobs view](https://github.com/data-derp/small-exercises/blob/master/databricks-spark-certification/assets/rdd-partitions.png?raw=true)
# MAGIC <br>[1] Jobs view

# COMMAND ----------

# By default distributed in 8 partitions
distData.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Let's read a file into a RDD and do some simple operations !

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _All the datasets used in this material are availble in Databricks by default !_

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 _It's always good to peak into the data that you are about to read_.

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks-datasets/weather/high_temps

# COMMAND ----------

file_path = 'dbfs:/databricks-datasets/weather/high_temps'
# reading a text file
data = sc.textFile(file_path)

# COMMAND ----------

# splitting each row of the file by the delimiter
pairs = data.map(lambda s: s.split(','))
pairs.collect()

# COMMAND ----------

#let us get maximum temperature by month
filter_header = pairs.filter(lambda s: "date" not in s[0])
#get the month from the date
get_month = filter_header.map(lambda s: [s[0].split('-')[1], float(s[1])])
#Get the maximum temperature by month
get_month.reduceByKey(lambda a,b:max(a,b)).collect()

# COMMAND ----------

# DBTITLE 1,Spark Configuration
# MAGIC %md
# MAGIC 
# MAGIC Spark contains a lot of configurable properties in the following catagories:
# MAGIC 1. Application properties
# MAGIC 2. Runtime environment
# MAGIC 3. Shuffle behavior
# MAGIC 4. Execution behavior
# MAGIC 5. Compression and serialization etc.
# MAGIC 
# MAGIC Let us look at a few of them.

# COMMAND ----------

# Use the following command  to list all the properties 
sc.getConf().getAll()

# COMMAND ----------

# DBTITLE 1,Getting and Setting properties
#read data
df = spark.read\
  .parquet('/databricks-datasets/amazon/data20K')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Let's get and set the shuffle property and obeserve how the changes in the output

# COMMAND ----------

# MAGIC %md
# MAGIC 💡
# MAGIC _**What is shuffle ?**_
# MAGIC <br>
# MAGIC _Data in spark is distributed over several nodes. Some operations require that coherent data be brought at one place which requires data exchange between nodes. This **data exchange is known as shuffle**. For example, say I want to calculate average temperature by city. The best way to do that is bringing records belonging to each city together and doing average._

# COMMAND ----------

'''
By default spark partitions data into 200 parts (some exceptions here)
'''
print(spark.conf.get('spark.sql.shuffle.partitions'))

# COMMAND ----------

# MAGIC %md
# MAGIC 💡
# MAGIC _**What is partitions ?**_
# MAGIC <br>
# MAGIC _Partitions in spark is data broken down into smaller blocks_

# COMMAND ----------

"""
Expand the Spark Job to see the partitions created in the last stage
of processing. You will notice that by default the shuffle partitions 
are '200'
"""
df.groupBy(df.rating).count().write.mode('overwrite').parquet(f'{working_directory}/tmp/test')

# COMMAND ----------

"""
Let us change the number of partitions that are used during that shuffle
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

# COMMAND ----------

# DBTITLE 1,Exercise 
# MAGIC %md
# MAGIC **Scenario 3.1**: Using [parallelize](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.parallelize.html) function change the number of partitions the RDD is initialised with. Obeserve the output by expanding 'Spark Jobs->Job ->Stage' 

# COMMAND ----------

'''
Write code here
'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution (hidden)**

# COMMAND ----------

# Create data into a distributed data structure (RDD) with 5 partitions
data = list(range(1,100))
distData = sc.parallelize(data, 5)
distData.collect()

# COMMAND ----------

# DBTITLE 1,References
# MAGIC %md
# MAGIC [1] [RDD programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
# MAGIC <br>
# MAGIC [2] [PySpark API](https://spark.apache.org/docs/latest/api/python/pyspark.html)
# MAGIC <br>
# MAGIC [3] [Spark Shuffle](https://0x0fff.com/spark-architecture-shuffle/)