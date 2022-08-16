# Databricks notebook source
# DBTITLE 0,SparkContext
# MAGIC %md
# MAGIC 
# MAGIC # SparkContext and Simple RDDs
# MAGIC 
# MAGIC 💡 _Starting with Spark 2.0, SparkContext is now available through the SparkSession endpoint (but it is still important to understand)_

# COMMAND ----------

# MAGIC %md
# MAGIC - When executing a Spark application, we must interact with the cluster using a SparkSession.
# MAGIC - Using SparkSession, you can access all of low-level and legacy contexts and configurations accordingly
# MAGIC - A SparkContext object within SparkSession represents connection with the spark cluster. This class is how you communicate with some of Spark’s lower-level APIs, such as RDDs. (SparkSession can have supports different contexts like HiveContext, SQLContext etc.)
# MAGIC 
# MAGIC 💡 An [RDD (or Resilient Distributed Dataset)](https://www.databricks.com/glossary/what-is-rdd) is an abstraction that Spark provides over data distributed accross nodes. Think of it as a distributed data structure. Later forms (DataSets, DataFrames) are all based on RDDs.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup
# MAGIC Before we get started, let's quickly set our working directory and pull in some data to play with.

# COMMAND ----------

# MAGIC %run ./init

# COMMAND ----------

# MAGIC %run ./init_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Spark Context with a simple RDD

# COMMAND ----------

# DBTITLE 0,Using SparkContext
spark_context = spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 You can technically also access the SparkContext using the `sc` variable in Databricks but since we're talking about accessing the SparkContext through the SparkSession endpoint, let's not use the special Databricks variable `sc`.

# COMMAND ----------

data = [1, 2, 3, 4, 5]

# COMMAND ----------

# MAGIC %md
# MAGIC Create a distributed dataset in the form an on RDD using [parallelize](https://spark.apache.org/docs/2.1.1/programming-guide.html#parallelized-collections).

# COMMAND ----------

distData = spark_context.parallelize(data, 2)
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
# MAGIC ## Simple operations on a RDD from a file
# MAGIC Let's read a file into a RDD and do some simple operations !

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

# Reading a text file through the Spark Context
data = spark_context.textFile(file_path)

# COMMAND ----------

# Split each row of the file by the delimiter
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

# DBTITLE 0,Exercise 
# MAGIC %md
# MAGIC 
# MAGIC ## Exercise
# MAGIC 
# MAGIC **Scenario 3a.1**: Using [parallelize](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.parallelize.html) function change the number of partitions the RDD is initialised with. Obeserve the output by expanding 'Spark Jobs->Job ->Stage' 

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

# MAGIC %md
# MAGIC ## References
# MAGIC 
# MAGIC * [RDD programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
# MAGIC * [PySpark API](https://spark.apache.org/docs/latest/api/python/pyspark.html)